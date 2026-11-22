using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SiemensPlcServer.Hubs;
using SiemensPlcServer.Models;
using SiemensPlcServer.Services;

namespace SiemensPlcServer.BackgroundServices;

/// <summary>
/// Раскладка UDP-пакета
/// </summary>
public sealed class TelemetryField
{
    [JsonPropertyName("name")]   public string Name { get; set; } = "";
    [JsonPropertyName("offset")] public int Offset { get; set; }
    [JsonPropertyName("type")]   public string Type { get; set; } = "u16";
    [JsonPropertyName("bit")]    public int Bit { get; set; }
    [JsonPropertyName("length")] public int Length { get; set; } = 1;
}

public sealed class TelemetryLayout
{
    [JsonPropertyName("length")] public int Length { get; set; } = 512;
    [JsonPropertyName("fields")] public List<TelemetryField> Fields { get; set; } = new();

    public static TelemetryLayout Load(string path, ILogger log)
    {
        if (!File.Exists(path))
        {
            log.LogWarning("Layout {Path} не найден — UDP-телеметрия разбирается как raw hex", path);
            return new TelemetryLayout();
        }
        var layout = JsonSerializer.Deserialize<TelemetryLayout>(File.ReadAllText(path))
                     ?? new TelemetryLayout();
        log.LogInformation("UDP layout: {Count} полей, {Len} байт", layout.Fields.Count, layout.Length);
        return layout;
    }

    /// Пакет big-endian (ТЗ §10.5). NaN/Inf -> null.
    /// Парсинг big-endian 
    public Dictionary<string, object?> Parse(ReadOnlySpan<byte> b)
    {
        var d = new Dictionary<string, object?>(Fields.Count);
        foreach (var f in Fields)
        {
            try
            {
                object? v = f.Type.ToLowerInvariant() switch
                {
                    "u8"  => b[f.Offset],
                    "i8"  => (sbyte)b[f.Offset],
                    "u16" => BinaryPrimitives.ReadUInt16BigEndian(b.Slice(f.Offset, 2)),
                    "i16" => BinaryPrimitives.ReadInt16BigEndian(b.Slice(f.Offset, 2)),
                    "u32" => BinaryPrimitives.ReadUInt32BigEndian(b.Slice(f.Offset, 4)),
                    "i32" => BinaryPrimitives.ReadInt32BigEndian(b.Slice(f.Offset, 4)),
                    "u64" => BinaryPrimitives.ReadUInt64BigEndian(b.Slice(f.Offset, 8)),
                    "f32" => Finite(BinaryPrimitives.ReadSingleBigEndian(b.Slice(f.Offset, 4))),
                    "f64" => Finite(BinaryPrimitives.ReadDoubleBigEndian(b.Slice(f.Offset, 8))),
                    "bit" => (b[f.Offset] & (1 << f.Bit)) != 0,
                    "ascii" => Encoding.ASCII.GetString(b.Slice(f.Offset, f.Length)).TrimEnd('\0', ' '),
                    _ => null
                };
                d[f.Name] = v;
            }
            catch { d[f.Name] = null; }   // поле вылезло за границы пакета
        }
        return d;
    }

private static object? Finite(float  v) => float.IsFinite(v)  ? v : null;
private static object? Finite(double v) => double.IsFinite(v) ? v : null;
}

/// <summary>
/// Приём UDP
/// 
/// </summary>
public sealed class UdpTelemetryReceiver : BackgroundService
{
    private readonly TelemetryOptions _opt;
    private readonly PlcDataStore _store;
    private readonly PlcCommunicationService _plc;
    private readonly IHubContext<PlcHub> _hub;
    private readonly TelemetryLayout _layout;
    private readonly ILogger<UdpTelemetryReceiver> _log;

    public UdpTelemetryReceiver(IOptions<TelemetryOptions> opt, PlcDataStore store,
        PlcCommunicationService plc, IHubContext<PlcHub> hub, TelemetryLayout layout,
        ILogger<UdpTelemetryReceiver> log)
    { _opt = opt.Value; _store = store; _plc = plc; _hub = hub; _layout = layout; _log = log; }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (!_opt.Enabled) { _log.LogInformation("UDP-телеметрия выключена"); return; }

        if (_opt.ConfigurePlcOnStartup)
            _ = Task.Run(() => ConfigurePlcAsync(ct), ct);

        using var udp = new UdpClient(new IPEndPoint(IPAddress.Parse(_opt.BindAddress), _opt.Port));
        _log.LogInformation("Слушаю UDP {Addr}:{Port}, ожидаю {Len} байт",
            _opt.BindAddress, _opt.Port, _opt.ExpectedLength);

        var lastBroadcast = DateTime.MinValue;
        var staleReported = false;

        while (!ct.IsCancellationRequested)
        {
            using var tick = CancellationTokenSource.CreateLinkedTokenSource(ct);
            tick.CancelAfter(500);   // Watchdog чтобы проверять «протухание» потока даже без пакетов
            try
            {
                var r = await udp.ReceiveAsync(tick.Token);

                if (r.Buffer.Length != _opt.ExpectedLength)
                {
                    _store.CountBad();
                    _log.LogWarning("UDP: длина {Len} != {Exp} от {Ep}",
                        r.Buffer.Length, _opt.ExpectedLength, r.RemoteEndPoint);
                    continue;
                }

                var values = _layout.Parse(r.Buffer);
                var seq = values.TryGetValue("seq", out var s) && s is not null
                          ? Convert.ToInt64(s) : _store.PacketsRx;
                var frame = new TelemetryFrame(seq, DateTime.UtcNow, values);
                _store.SetTelemetry(frame);
                staleReported = false;

                // Коалесцирование пакетов: 
                // ограничивается BroadcastPeriodMs, промежуточные кадры отбрасываются
                if ((DateTime.UtcNow - lastBroadcast).TotalMilliseconds >= _opt.BroadcastPeriodMs)
                {
                    lastBroadcast = DateTime.UtcNow;
                    await _hub.Clients.Group(PlcHub.TelemetryGroup).SendAsync("telemetry", new
                    {
                        seq = frame.Seq, ts = frame.RxUtc,
                        plcTsMs = values.GetValueOrDefault("plc_ts_ms"),
                        values = frame.Values
                    }, ct);
                }
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                var t = _store.Telemetry;
                if (!staleReported && (t is null ||
                    (DateTime.UtcNow - t.RxUtc).TotalMilliseconds > _opt.StaleAfterMs))
                {
                    staleReported = true;
                    _log.LogWarning("UDP-телеметрия протухла — проверьте Settings_DB.telemetry.enabled и firewall");
                    await _hub.Clients.Group(PlcHub.TelemetryGroup)
                        .SendAsync("telemetryLost", new { ts = DateTime.UtcNow }, ct);
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _log.LogError(ex, "UDP приём");
                await Task.Delay(200, ct);
            }
        }
    }

    /// ПЛК по умолчанию telemetry.enabled = FALSE 
    /// Включается, когда сервер готов слушать.
    private async Task ConfigurePlcAsync(CancellationToken ct)
    {
        const string root = "\"Settings_DB\".\"telemetry\"";
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var ip = IPAddress.Parse(_opt.ServerIp).GetAddressBytes();
                for (var i = 0; i < 4; i++)
                    await _plc.WriteAsync($"{root}.\"dest_ip\"[{i + 1}]", ip[i], ct);

                await _plc.WriteAsync($"{root}.\"dest_port\"", (ushort)_opt.Port, ct);
                await _plc.WriteAsync($"{root}.\"period_ms\"",
                    (short)Math.Clamp(_opt.PeriodMs, 20, 1000), ct);
                await _plc.WriteAsync($"{root}.\"plc_interface\"", (short)_opt.PlcInterface, ct);
                await _plc.WriteAsync($"{root}.\"enabled\"", true, ct);   // последним!

                _log.LogInformation("ПЛК настроен на push {Ip}:{Port} каждые {Ms} мс (X{If})",
                    _opt.ServerIp, _opt.Port, _opt.PeriodMs, _opt.PlcInterface);
                return;
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Не удалось настроить telemetry в ПЛК, повтор через 10 с");
                await Task.Delay(10000, ct);
            }
        }
    }
}