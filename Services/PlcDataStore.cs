using System.Collections.Concurrent;
using Newtonsoft.Json.Linq;

namespace SiemensPlcServer.Services;

public sealed record TagValue(object? Value, DateTime TsUtc, bool Good, string? Error);
public sealed record TelemetryFrame(long Seq, DateTime RxUtc, IReadOnlyDictionary<string, object?> Values);

public sealed class PlcDataStore
{
    private readonly ConcurrentDictionary<string, TagValue> _tags = new();
    private TelemetryFrame? _telemetry;
    private long _rx, _bad, _gaps, _lastSeq = -1;

    public TelemetryFrame? Telemetry => Volatile.Read(ref _telemetry);
    public long PacketsRx => Interlocked.Read(ref _rx);
    public long PacketsBad => Interlocked.Read(ref _bad);
    public long SeqGaps => Interlocked.Read(ref _gaps);

    public IReadOnlyDictionary<string, TagValue> Snapshot(IEnumerable<string> tags) =>
        tags.Where(_tags.ContainsKey).ToDictionary(t => t, t => _tags[t]);

    public IReadOnlyDictionary<string, TagValue> All => _tags;

    /// Возвращает только изменившиеся теги — наружу уходит delta, а не весь снапшот.
    public Dictionary<string, TagValue> ApplyAndDiff(IReadOnlyDictionary<string, TagReadResult> read)
    {
        var now = DateTime.UtcNow;
        var delta = new Dictionary<string, TagValue>();
        foreach (var (tag, r) in read)
        {
            var v = new TagValue(Sanitize(r.Value), now, r.Good, r.Error);
            var changed = !_tags.TryGetValue(tag, out var old)
                          || old.Good != v.Good
                          || !Same(old.Value, v.Value);
            _tags[tag] = v;
            if (changed) delta[tag] = v;
        }
        return delta;
    }

    public void SetTelemetry(TelemetryFrame frame)
    {
        if (_lastSeq >= 0 && frame.Seq > _lastSeq + 1)
            Interlocked.Add(ref _gaps, frame.Seq - _lastSeq - 1);
        _lastSeq = frame.Seq;
        Interlocked.Increment(ref _rx);
        Volatile.Write(ref _telemetry, frame);
    }

    public void CountBad() => Interlocked.Increment(ref _bad);

    public object Status(bool plcConnected, string? plcError, int staleAfterMs)
    {
        var t = Telemetry;
        var age = t is null ? (double?)null : (DateTime.UtcNow - t.RxUtc).TotalMilliseconds;
        return new
        {
            plcConnected, plcError,
            telemetryAlive = age is not null && age < staleAfterMs,
            telemetryAgeMs = age,
            packetsRx = PacketsRx, packetsBad = PacketsBad, seqGaps = SeqGaps,
            tagCount = _tags.Count,
            serverTs = DateTime.UtcNow
        };
    }

    // NaN/Inf из ПЛК (валидный JSON их не имеет) -> null
    private static object? Sanitize(object? v) => v switch
    {
        double d => double.IsFinite(d) ? d : null,
        float f => float.IsFinite(f) ? f : null,
        _ => v
    };

    private static bool Same(object? a, object? b)
    {
        if (a is null || b is null) return a is null && b is null;
        if (a is JToken ja && b is JToken jb) return JToken.DeepEquals(ja, jb);
        return a.Equals(b);
    }

    /// Помечает теги группы недостоверными, СОХРАНЯЯ последнее значение и его метку времени.
/// Возвращает только те, что реально сменили статус — их и рассылаем как delta.
public Dictionary<string, TagValue> MarkBad(IEnumerable<string> tags, string error)
{
    var delta = new Dictionary<string, TagValue>();
    foreach (var tag in tags)
    {
        if (!_tags.TryGetValue(tag, out var old) || !old.Good) continue;
        var v = old with { Good = false, Error = error };
        _tags[tag] = v;
        delta[tag] = v;
    }
    return delta;
}
}