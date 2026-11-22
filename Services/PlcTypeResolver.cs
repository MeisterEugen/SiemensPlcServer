using System.Collections.Concurrent;
using Siemens.Simatic.S7.Webserver.API.Models;

namespace SiemensPlcServer.Services;

/// Тип тега для записи: сначала кэш каталога, затем PlcProgram.Browse родителя.
public sealed class PlcTypeResolver
{
    private readonly PlcCommunicationService _plc;
    private readonly PlcTagCatalog _catalog;
    private readonly ILogger<PlcTypeResolver> _log;
    private readonly ConcurrentDictionary<string, string> _cache = new(StringComparer.Ordinal);

    public PlcTypeResolver(PlcCommunicationService plc, PlcTagCatalog catalog, ILogger<PlcTypeResolver> log)
    { _plc = plc; _catalog = catalog; _log = log; }

    public async Task<string?> ResolveAsync(string tag, CancellationToken ct)
    {
        if (_cache.TryGetValue(tag, out var cached)) return cached;
        if (_catalog.TryGetType(tag, out var known)) { _cache[tag] = known; return known; }

        var dot = tag.LastIndexOf('.');
        if (dot <= 0) return null;
        var parent = tag[..dot];
        var leaf = tag[(dot + 1)..];

        try
        {
            foreach (var child in await _plc.BrowseChildrenAsync(parent, ct))
            {
                var node = Match(child, leaf);
                if (node is null) continue;

                var type = node.Datatype.ToString();
                _cache[tag] = type;
                _log.LogInformation("Тип {Tag} = {Type}", tag, type);
                return type;
            }
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Не удалось определить тип {Tag}", tag);
        }
        return null;
    }

    private static ApiPlcProgramData? Match(ApiPlcProgramData node, string leaf)
    {
        if (node.GetNameWithQuotes() == leaf) return node;
        // массив: элементы уже названы "name"[i]
        return node.ArrayElements?.FirstOrDefault(e => e.GetNameWithQuotes() == leaf);
    }
}