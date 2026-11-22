using Microsoft.Extensions.Options;
using Siemens.Simatic.S7.Webserver.API.Enums;
using Siemens.Simatic.S7.Webserver.API.Models;
using Siemens.Simatic.S7.Webserver.API.Models.Requests;
using SiemensPlcServer.Models;

namespace SiemensPlcServer.Services;

public sealed record TagReadResult(object? Value, bool Good, string? Error);

public sealed class PlcCommunicationService
{
    private readonly PlcConnection _conn;
    private readonly PlcOptions _opt;
    private readonly ILogger<PlcCommunicationService> _log;

    public PlcCommunicationService(PlcConnection conn, IOptions<PlcOptions> opt,
                                   ILogger<PlcCommunicationService> log)
    { _conn = conn; _opt = opt.Value; _log = log; }

    public bool IsConnected => _conn.IsConnected;
    public string? LastError => _conn.LastError;

    /// Один HTTPS-раундtrip на chunk тегов. Ошибка одного тега не ломает пакет.
    public async Task<IReadOnlyDictionary<string, TagReadResult>> ReadTagsAsync(
        IReadOnlyList<string> tags, CancellationToken ct)
    {
        var result = new Dictionary<string, TagReadResult>(tags.Count);
        if (tags.Count == 0) return result;

        foreach (var chunk in tags.Chunk(Math.Max(1, _opt.MaxTagsPerBulk)))
        {
            ct.ThrowIfCancellationRequested();
            var rf = _conn.RequestFactory;

            List<IApiRequest> requests = chunk
                .Select(t => rf.GetApiPlcProgramReadRequest(t, ApiPlcDataRepresentation.Simple))
                .ToList();
            requests = rf.GetApiBulkRequestWithUniqueIds(requests).ToList();

            // id -> символьное имя; порядок ответа НЕ гарантирован, матчим по id
            var byId = requests.ToDictionary(r => r.Id, r => (string)r.Params!["var"]);

            var response = await _conn.ExecuteAsync((h, t) => h.ApiBulkAsync(requests, t), ct);

            foreach (var ok in response.SuccessfulResponses)
                if (byId.TryGetValue(ok.Id, out var tag))
                    result[tag] = new TagReadResult(ok.Result, true, null);

            foreach (var bad in response.ErrorResponses)
                if (byId.TryGetValue(bad.Id, out var tag))
                {
                    result[tag] = new TagReadResult(null, false,
                        $"{bad.Error?.Code}: {bad.Error?.Message}");
                    _log.LogDebug("Тег {Tag}: {Err}", tag, result[tag].Error);
                }

            foreach (var tag in chunk)
                result.TryAdd(tag, new TagReadResult(null, false, "нет ответа в bulk"));
        }
        return result;
    }

    

public Task<object?> ReadOneAsync(string tag, CancellationToken ct) =>
    _conn.ExecuteAsync(async (h, t) =>
        (object?)(await h.PlcProgramReadAsync<object>(tag, ApiPlcDataRepresentation.Simple, t)).Result, ct);

public Task WriteAsync(string tag, object value, CancellationToken ct) =>
    _conn.ExecuteAsync((h, t) => h.PlcProgramWriteAsync(tag, value, ApiPlcDataRepresentation.Simple, t), ct);

public Task<List<ApiPlcProgramData>> BrowseChildrenAsync(string? varPath, CancellationToken ct) =>
    _conn.ExecuteAsync(async (h, t) =>
        (await h.PlcProgramBrowseAsync(ApiPlcProgramBrowseMode.Children, varPath, t)).Result, ct);
}