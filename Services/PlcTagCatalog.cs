using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;
using Siemens.Simatic.S7.Webserver.API.Models;
using SiemensPlcServer.Models;

namespace SiemensPlcServer.Services;

public sealed record ResolvedGroup(string Name, int PeriodMs, IReadOnlyList<string> Tags, bool FullyResolved);

/// <summary>
/// Каталог тегов по группам: offline-фаза + online-раскрытие
/// </summary>
public sealed class PlcTagCatalog
{
    private readonly PlcCommunicationService _plc;
    private readonly PlcOptions _opt;
    private readonly ILogger<PlcTagCatalog> _log;
    private readonly ConcurrentDictionary<string, ResolvedGroup> _groups = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _types = new(StringComparer.Ordinal);
    private int _revision;
    public bool TryGetType(string tag, out string type) => _types.TryGetValue(tag, out type!);

    public PlcTagCatalog(PlcCommunicationService plc, IOptions<PlcOptions> opt, ILogger<PlcTagCatalog> log)
    { _plc = plc; _opt = opt.Value; _log = log; }

    public int Revision => Volatile.Read(ref _revision);
    public IReadOnlyDictionary<string, ResolvedGroup> Groups => _groups;
    public IEnumerable<string> Names => _groups.Keys;
    public bool HasGroup(string name) => _groups.ContainsKey(name);
    public bool AllResolved => _groups.Values.All(g => g.FullyResolved);
    public IReadOnlyList<string> TagsOf(string name) =>
        _groups.TryGetValue(name, out var g) ? g.Tags : Array.Empty<string>();

    // ── ФАЗА 1: чистая арифметика имён, ПЛК НЕ НУЖЕН. Вызвать ДО app.Run() ──
    public void BuildOffline()
    {
        foreach (var g in _opt.Groups)
        {
            var tags = new List<string>();
            var hasBrowse = false;
            foreach (var pattern in g.Tags)
            {
                if (IsBrowse(pattern)) { hasBrowse = true; continue; }
                tags.AddRange(ExpandRanges(pattern).Select(Normalize));
            }
            _groups[g.Name] = new ResolvedGroup(g.Name, g.PeriodMs, tags.Distinct().ToList(), !hasBrowse);
        }
        Interlocked.Increment(ref _revision);

        _log.LogInformation("Каталог offline: {G} групп, {T} тегов. Ждут Browse: {P}",
            _groups.Count, _groups.Values.Sum(g => g.Tags.Count),
            string.Join(", ", _groups.Values.Where(g => !g.FullyResolved).Select(g => g.Name)));
    }

    // ── ФАЗА 2: раскрытие ".*" через PlcProgram.Browse. Идемпотентна, можно звать в цикле ──
    public async Task ResolveOnlineAsync(CancellationToken ct)
    {
        foreach (var g in _opt.Groups)
        {
            ct.ThrowIfCancellationRequested();
            if (_groups.TryGetValue(g.Name, out var cur) && cur.FullyResolved) continue;

            var patterns = g.Tags.Where(IsBrowse).ToList();
            if (patterns.Count == 0) continue;

            var tags = cur.Tags.ToList();
            var allOk = true;

            foreach (var pattern in patterns)
            {
                try
                {
                    var found = new List<string>();
                    await ExpandByBrowseAsync(Normalize(pattern[..^2]), found, 0, ct);
                    tags.AddRange(found);
                    _log.LogInformation("{Pattern} → {N} тегов", pattern, found.Count);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                catch (Exception ex)
                {
                    allOk = false;
                    _log.LogWarning(ex, "Не раскрыл {Pattern} (группа {Group})", pattern, g.Name);
                }
            }

            _groups[g.Name] = new ResolvedGroup(g.Name, g.PeriodMs, tags.Distinct().ToList(), allOk);
            Interlocked.Increment(ref _revision);
        }
    }

    private static bool IsBrowse(string p) => p.EndsWith(".*", StringComparison.Ordinal);

    // "NonRetain_DB.state.hv_on[1]" -> "\"NonRetain_DB\".\"state\".\"hv_on\"[1]"
    public static string Normalize(string path)
    {
        var sb = new StringBuilder();
        foreach (var raw in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            var seg = raw.Trim();
            var br = seg.IndexOf('[');
            var name = (br < 0 ? seg : seg[..br]).Trim('"');
            var idx = br < 0 ? "" : seg[br..];
            if (sb.Length > 0) sb.Append('.');
            sb.Append('"').Append(name).Append('"').Append(idx);
        }
        return sb.ToString();
    }

    public static IEnumerable<string> ExpandRanges(string path)
    {
        var m = Regex.Match(path, @"\[(\d+)\.\.(\d+)\]");
        if (!m.Success) { yield return path; yield break; }
        int lo = int.Parse(m.Groups[1].Value), hi = int.Parse(m.Groups[2].Value);
        if (hi - lo > 4096) throw new InvalidOperationException($"Слишком широкий диапазон: {path}");
        for (var i = lo; i <= hi; i++)
            foreach (var p in ExpandRanges(path[..m.Index] + $"[{i}]" + path[(m.Index + m.Length)..]))
                yield return p;
    }

    private async Task ExpandByBrowseAsync(string basePath, List<string> sink, int depth, CancellationToken ct)
    {
        if (depth > 6 || sink.Count >= _opt.MaxExpandTags) return;
        foreach (var child in await _plc.BrowseChildrenAsync(basePath, ct))
            await WalkAsync(basePath, child, sink, depth + 1, ct);
    }

    private async Task WalkAsync(string parentPath, ApiPlcProgramData node,
                                 List<string> sink, int depth, CancellationToken ct)
    {
        if (sink.Count >= _opt.MaxExpandTags || depth > 8) return;

        if (node.ArrayElements is { Count: > 0 })
        {
            if (node.ArrayElements.Count > _opt.MaxExpandTags)
            {
                _log.LogWarning("Пропускаю массив {Path} ({N} эл.) — читать по запросу или через UDP",
                    parentPath + "." + node.GetNameWithQuotes(), node.ArrayElements.Count);
                return;
            }
            foreach (var el in node.ArrayElements)
                await WalkAsync(parentPath, el, sink, depth + 1, ct);
            return;
        }

        var path = parentPath + "." + node.GetNameWithQuotes();
if (node.Has_children == true)
    await ExpandByBrowseAsync(path, sink, depth, ct);
else
{
    sink.Add(path);
    _types[path] = node.Datatype.ToString();   // ← запоминаем тип
}
    }
}