using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SiemensPlcServer.Hubs;
using SiemensPlcServer.Models;

namespace SiemensPlcServer.Services;

public sealed record WriteJob(string Tag, JsonElement Value, string RequestId,
                              string ConnectionId, int PulseMs, string? User);

/// <summary>
/// Ограниченная (bounded) очередь команд. Защита от перегрузки: при заполнении
/// 256 слотов производитель ждёт (backpressure), а не роняет сервер.
/// </summary>
public sealed class PlcWriteQueue
{
    private readonly Channel<WriteJob> _ch = Channel.CreateBounded<WriteJob>(
        new BoundedChannelOptions(256) { FullMode = BoundedChannelFullMode.Wait });

    public ValueTask EnqueueAsync(WriteJob job, CancellationToken ct) => _ch.Writer.WriteAsync(job, ct);
    public IAsyncEnumerable<WriteJob> ReadAllAsync(CancellationToken ct) => _ch.Reader.ReadAllAsync(ct);
}

/// <summary>
/// Политика записи: allowlist тегов + API-ключ.
/// Запись в конфиге с точкой на конце ("...\"cmd\".") = всё поддерево;
/// без точки = ровно один тег.
/// </summary>
public sealed class PlcWritePolicy
{
    private readonly PlcOptions _opt;
    public PlcWritePolicy(IOptions<PlcOptions> opt) => _opt = opt.Value;

    public bool IsAllowed(string normalizedTag) =>
        _opt.WriteAllow.Any(p =>
        {
            var subtree = p.EndsWith('.');
            var norm = PlcTagCatalog.Normalize(p.TrimEnd('.'));
            return subtree
                ? normalizedTag.StartsWith(norm + ".", StringComparison.Ordinal)
                : normalizedTag.Equals(norm, StringComparison.Ordinal);
        });

    public bool KeyOk(string? provided) =>
        string.IsNullOrEmpty(_opt.WriteApiKey) || provided == _opt.WriteApiKey;
}

/// <summary>
/// Единственный потребитель очереди: политика → тип тега → приведение → запись → аудит.
/// </summary>
public sealed class PlcWriteWorker : BackgroundService
{
    private readonly PlcWriteQueue _queue;
    private readonly PlcCommunicationService _plc;
    private readonly PlcWritePolicy _policy;
    private readonly PlcTypeResolver _types;          // ← вот чего не хватало
    private readonly IHubContext<PlcHub> _hub;
    private readonly ILogger<PlcWriteWorker> _log;

    public PlcWriteWorker(
        PlcWriteQueue queue,
        PlcCommunicationService plc,
        PlcWritePolicy policy,
        PlcTypeResolver types,                        // ← и вот этого параметра
        IHubContext<PlcHub> hub,
        ILogger<PlcWriteWorker> log)
    {
        _queue = queue;
        _plc = plc;
        _policy = policy;
        _types = types;
        _hub = hub;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var job in _queue.ReadAllAsync(ct))
            {
                var error = await ExecuteJobAsync(job, ct);

                try
                {
                    await _hub.Clients.Client(job.ConnectionId).SendAsync("writeResult",
                        new { requestId = job.RequestId, tag = job.Tag, ok = error is null, error }, ct);
                }
                catch (OperationCanceledException) { break; }   // клиент отвалился / хост гасится
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _log.LogInformation("Писатель команд остановлен");
        }
    }

    private async Task<string?> ExecuteJobAsync(WriteJob job, CancellationToken ct)
    {
        try
        {
            if (!_policy.IsAllowed(job.Tag))
                throw new InvalidOperationException("тег вне WriteAllow");

            var plcType = await _types.ResolveAsync(job.Tag, ct);
            if (plcType is null)
                _log.LogWarning("Тип {Tag} не определён — привожу эвристикой", job.Tag);

            if (job.PulseMs > 0)
            {
                if (plcType is not null && !plcType.Equals("Bool", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException($"Импульс только по BOOL, у {job.Tag} тип {plcType}");

                var ms = Math.Clamp(job.PulseMs, 50, 2000);
                _log.LogWarning("AUDIT импульс {Tag} {Ms} мс (conn {Conn}, user {User})",
                    job.Tag, ms, job.ConnectionId, job.User ?? "-");

                await _plc.WriteAsync(job.Tag, true, ct);
                try
                {
                    await Task.Delay(ms, ct);
                }
                finally
                {
                    // Снимаем импульс даже при остановке хоста, но с ограничением по времени,
                    // чтобы не зависнуть на гейте PlcConnection навсегда.
                    try
                    {
                        using var reset = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                        await _plc.WriteAsync(job.Tag, false, reset.Token);
                    }
                    catch (Exception ex)
                    {
                        _log.LogCritical(ex,
                            "НЕ удалось снять импульс {Tag} — тег мог залипнуть в TRUE!", job.Tag);
                    }
                }
            }
            else
            {
                var value = PlcValueConverter.FromJson(job.Value, plcType);

                _log.LogWarning("AUDIT запись {Tag} = {Val} ({PlcType} -> {ClrType}, conn {Conn}, user {User})",
                    job.Tag, value, plcType ?? "?", value.GetType().Name,
                    job.ConnectionId, job.User ?? "-");

                await _plc.WriteAsync(job.Tag, value, ct);
            }

            return null;
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Запись {Tag} не удалась", job.Tag);
            return ex.Message;
        }
    }

    
}