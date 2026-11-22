using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SiemensPlcServer.Hubs;
using SiemensPlcServer.Models;
using SiemensPlcServer.Services;

namespace SiemensPlcServer.BackgroundServices;

/// <summary>
/// Циклический опрос ПЛК группами (bulk JSON-RPC через PlcProgram.Read) и рассылка
/// изменений подписчикам SignalR. Быстрые данные сюда НЕ попадают — их пушит сам ПЛК
/// по UDP (Settings_DB.telemetry), их принимает UdpTelemetryReceiver.
/// </summary>
public sealed class PlcMonitoringService : BackgroundService
{
    /// Имя группы в Plc:Groups, для которой включается детектор фронтов событий.
    private const string EventsGroupName = "events";

    private readonly PlcCommunicationService _plc;
    private readonly PlcTagCatalog _catalog;
    private readonly PlcDataStore _store;
    private readonly IHubContext<PlcHub> _hub;
    private readonly PlcOptions _plcOptions;
    private readonly TelemetryOptions _telemetryOptions;
    private readonly ILogger<PlcMonitoringService> _log;

    // Последние состояния событий. Трогается только из цикла группы "events" —锁 не нужен.
    private readonly Dictionary<string, bool> _eventEdges = new();

    public PlcMonitoringService(
        PlcCommunicationService plc,
        PlcTagCatalog catalog,
        PlcDataStore store,
        IHubContext<PlcHub> hub,
        IOptions<PlcOptions> plcOptions,
        IOptions<TelemetryOptions> telemetryOptions,
        ILogger<PlcMonitoringService> log)
    {
        _plc = plc;
        _catalog = catalog;
        _store = store;
        _hub = hub;
        _plcOptions = plcOptions.Value;
        _telemetryOptions = telemetryOptions.Value;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var enabled = _plcOptions.Groups.Where(g => g.Enabled).Select(g => g.Name)
                            .ToHashSet(StringComparer.OrdinalIgnoreCase);

            // Группы берём из offline-каталога — они уже есть, даже если ПЛК выключен
            var groups = _catalog.Groups.Values
                .Where(g => g.PeriodMs > 0 && enabled.Contains(g.Name))
                .ToList();

            if (groups.Count == 0)
            {
                _log.LogWarning("Нет активных групп — проверьте секцию Plc:Groups (Enabled/PeriodMs)");
                return;
            }

            var loops = new List<Task>
            {
                StatusLoopAsync(stoppingToken),
                ResolveCatalogLoopAsync(stoppingToken)   // Browse — в фоне, не блокирует опрос
            };
            for (var i = 0; i < groups.Count; i++)
                loops.Add(GroupLoopAsync(groups[i].Name, groups[i].PeriodMs,
                                         TimeSpan.FromMilliseconds(i * 60), stoppingToken));

            _log.LogInformation("Мониторинг запущен: {N} групп", groups.Count);
            await Task.WhenAll(loops);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        catch (Exception ex) { _log.LogCritical(ex, "Мониторинг ПЛК аварийно завершился"); }
    }  
    private async Task ResolveCatalogLoopAsync(CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested && !_catalog.AllResolved)
        {
            attempt++;
            try
            {
                await _catalog.ResolveOnlineAsync(ct);
                if (_catalog.AllResolved)
                {
                    _log.LogInformation("Каталог раскрыт полностью: {N} тегов",
                        _catalog.Groups.Values.Sum(g => g.Tags.Count));
                    return;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch (Exception ex) { _log.LogWarning(ex, "Browse не удался, попытка {N}", attempt); }

            await Task.Delay(TimeSpan.FromSeconds(Math.Min(30, 3 * attempt)), ct);
            }
    }

    // ── 2. Цикл одной группы ──────────────────────────────────────────────────────
    private async Task GroupLoopAsync(string name, int periodMs, TimeSpan stagger, CancellationToken ct)
    {
        var period = TimeSpan.FromMilliseconds(Math.Max(50, periodMs));
        using var timer = new PeriodicTimer(period);
        var clients = _hub.Clients.Group(PlcHub.GroupOf(name));
        var isEvents = string.Equals(name, EventsGroupName, StringComparison.OrdinalIgnoreCase);
        var failures = 0;
        var lastRevision = -1;
        var emptyLogged = false;

        if (stagger > TimeSpan.Zero) await Task.Delay(stagger, ct);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                // Список может вырасти после дораскрытия ".*" — берём каждый цикл
                var tags = _catalog.TagsOf(name);
                if (tags.Count == 0)
                {
                    if (!emptyLogged)
                    {
                        emptyLogged = true;
                        _log.LogWarning("Группа {Group}: тегов пока нет, ждём PlcProgram.Browse", name);
                    }
                    continue;
                }
                emptyLogged = false;

                var started = Stopwatch.GetTimestamp();
                try
                {
                    var read = await _plc.ReadTagsAsync(tags, ct);
                    var delta = _store.ApplyAndDiff(read);
                    var revision = _catalog.Revision;
                    var fullSnapshot = failures > 0 || revision != lastRevision;
                    failures = 0;
                    lastRevision = revision;

                    if (fullSnapshot)
                        await clients.SendAsync("snapshot",
                            new { group = name, ts = DateTime.UtcNow, values = _store.Snapshot(tags) }, ct);
                    else if (delta.Count > 0)
                        await clients.SendAsync("delta",
                            new { group = name, ts = DateTime.UtcNow, values = delta }, ct);

                    if (isEvents) await EmitEventsAsync(delta, clients, ct);

                    var elapsed = Stopwatch.GetElapsedTime(started);
                    if (elapsed > period)
                        _log.LogWarning("Группа {Group}: цикл {E} мс > периода {P} мс",
                            name, (int)elapsed.TotalMilliseconds, (int)period.TotalMilliseconds);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
                catch (Exception ex)
                    {
                    failures++;
                    var stale = _store.MarkBad(tags, ex.Message);
                    if (stale.Count > 0)
                        await clients.SendAsync("delta",
                            new { group = name, ts = DateTime.UtcNow, values = stale }, ct);

                    if (failures == 1 || failures % 30 == 0)
                        _log.LogError(ex, "Группа {Group}: опрос не удался (подряд {N})", name, failures);

                    await Task.Delay(TimeSpan.FromMilliseconds(
                        Math.Min(5000, period.TotalMilliseconds * Math.Pow(2, Math.Min(failures, 5)))), ct);
                    }
                }
            }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        }
    // ── 3. Фронты событий (вкладка Events) ────────────────────────────────────────
    // Кнопки ЛПУ продлены ПЛК на 2 с (EVENT_HOLD) → период группы 250 мс ловит фронт
    // гарантированно. Состояния (затворы, рама, полоса, HV) — пишем ОБА фронта.
    private async Task EmitEventsAsync(
        IReadOnlyDictionary<string, TagValue> delta,
        IClientProxy clients,
        CancellationToken ct)
    {
        List<object>? journal = null;

        foreach (var (tag, tv) in delta)
        {
            if (!tv.Good || tv.Value is not bool now) continue;
            if (!PlcEventCatalog.TryGet(FieldName(tag), out var def)) continue;

            if (!_eventEdges.TryGetValue(tag, out var prev))
            {
                _eventEdges[tag] = now;   // первое чтение после старта — только запоминаем
                continue;
            }
            if (prev == now) continue;
            _eventEdges[tag] = now;

            if (!now && !def.BothEdges) continue;   // импульсные события — только 0→1

            journal ??= new List<object>();
            journal.Add(new
            {
                code = def.Code,                 // 0x2NII, уровень INFO
                name = FieldName(tag),
                on = now,
                ts = tv.TsUtc
            });
        }

        if (journal is not null)
            await clients.SendAsync("events", journal, ct);
    }

    private static string FieldName(string tag)
    {
        var i = tag.LastIndexOf('.');
        return (i < 0 ? tag : tag[(i + 1)..]).Trim('"');
    }

    // ── 4. Статус связи ───────────────────────────────────────────────────────────
    private async Task StatusLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        string? lastSignature = null;
        var lastSent = DateTime.MinValue;

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var t = _store.Telemetry;
                var alive = t is not null &&
                            (DateTime.UtcNow - t.RxUtc).TotalMilliseconds < _telemetryOptions.StaleAfterMs;

                // Сигнатура без счётчиков и без serverTs — иначе слали бы каждые 2 с всегда
                var signature = $"{_plc.IsConnected}|{_plc.LastError}|{alive}|{_store.All.Count}";
                var heartbeat = (DateTime.UtcNow - lastSent).TotalSeconds >= 10;

                if (signature == lastSignature && !heartbeat) continue;

                lastSignature = signature;
                lastSent = DateTime.UtcNow;
                await PushStatusAsync(ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private Task PushStatusAsync(CancellationToken ct) =>
        _hub.Clients.All.SendAsync("status",
            _store.Status(_plc.IsConnected, _plc.LastError, _telemetryOptions.StaleAfterMs), ct);
}