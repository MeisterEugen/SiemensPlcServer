using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SiemensPlcServer.Models;
using SiemensPlcServer.Services;

namespace SiemensPlcServer.Hubs;

public sealed class PlcHub : Hub
{
    public const string TelemetryGroup = "telemetry";
    public static string GroupOf(string name) => "plc:" + name;

    private readonly PlcTagCatalog _catalog;
    private readonly PlcDataStore _store;
    private readonly PlcWriteQueue _queue;
    private readonly PlcWritePolicy _policy;
    private readonly TelemetryOptions _tel;

    public PlcHub(PlcTagCatalog catalog, PlcDataStore store, PlcWriteQueue queue,
                  PlcWritePolicy policy, IOptions<TelemetryOptions> tel)
    { _catalog = catalog; _store = store; _queue = queue; _policy = policy; _tel = tel.Value; }

    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("status", _store.Status(true, null, _tel.StaleAfterMs));
        await Clients.Caller.SendAsync("groups", _catalog.Groups.Values.Select(g => new
        {
            name = g.Name, periodMs = g.PeriodMs, tagCount = g.Tags.Count, resolved = g.FullyResolved
        }));
        await base.OnConnectedAsync();
    }

    /// Клиент получает полный снапшот сразу, дальше — только delta.
    public async Task Subscribe(string[] groups)
    {
        var unknown = groups.Where(g => !_catalog.HasGroup(g)).ToArray();
        if (unknown.Length > 0)
            throw new HubException(
                $"Неизвестные группы: {string.Join(", ", unknown)}. " +
                $"Доступные: {string.Join(", ", _catalog.Names)}");
    
        foreach (var g in groups.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            await Groups.AddToGroupAsync(Context.ConnectionId, GroupOf(g));
            var tags = _catalog.TagsOf(g);
            await Clients.Caller.SendAsync("snapshot", new
            {
                group = g,
                ts = DateTime.UtcNow,
                resolved = _catalog.Groups[g].FullyResolved,   // false = часть тегов ещё раскрывается
                tagCount = tags.Count,
                values = _store.Snapshot(tags)                 // может быть пустым — это нормально
            });
        }
    }

    public async Task Unsubscribe(string[] groups)
    {
        foreach (var g in groups)
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, GroupOf(g));
    }

    public async Task SubscribeTelemetry()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, TelemetryGroup);
        var t = _store.Telemetry;
        if (t is not null)
            await Clients.Caller.SendAsync("telemetry", new { seq = t.Seq, ts = t.RxUtc, values = t.Values });
    }

    public Task UnsubscribeTelemetry() =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, TelemetryGroup);

    /// Значение или импульс (PulseMs>0) — для cmd.reset_errors, cmd.add_steel, jog и т.п.
    public async Task<string> Write(string tag, JsonElement value, int pulseMs = 0)
    {
        var http = Context.GetHttpContext();
        if (!_policy.KeyOk(http?.Request.Headers["X-Api-Key"]))
            throw new HubException("Неверный X-Api-Key");

        var normalized = PlcTagCatalog.Normalize(tag);
        if (!_policy.IsAllowed(normalized))
            throw new HubException($"Запись запрещена политикой: {normalized}");

        var requestId = Guid.NewGuid().ToString("N");
        await _queue.EnqueueAsync(
            new WriteJob(normalized, value, requestId, Context.ConnectionId, pulseMs,
                         Context.User?.Identity?.Name),
            Context.ConnectionAborted);
        return requestId;   // результат придёт событием writeResult
    }
}