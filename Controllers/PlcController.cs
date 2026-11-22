using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using SiemensPlcServer.BackgroundServices;
using SiemensPlcServer.Models;
using SiemensPlcServer.Services;

namespace SiemensPlcServer.Controllers;

[ApiController]
[Route("api/plc")]
public class PlcController : ControllerBase
{
    private readonly PlcCommunicationService _plc;
    private readonly PlcTagCatalog _catalog;
    private readonly PlcDataStore _store;
    private readonly PlcWriteQueue _queue;
    private readonly PlcWritePolicy _policy;
    private readonly TelemetryOptions _tel;

    public PlcController(PlcCommunicationService plc, PlcTagCatalog catalog, PlcDataStore store,
        PlcWriteQueue queue, PlcWritePolicy policy, IOptions<TelemetryOptions> tel)
    { _plc = plc; _catalog = catalog; _store = store; _queue = queue; _policy = policy; _tel = tel.Value; }

    [HttpGet("status")]
    public IActionResult Status() => Ok(_store.Status(_plc.IsConnected, _plc.LastError, _tel.StaleAfterMs));

    [HttpGet("groups")]
    public IActionResult Groups() => Ok(_catalog.Groups.Values
        .Select(g => new { g.Name, g.PeriodMs, tagCount = g.Tags.Count }));

    [HttpGet("snapshot")]
    public IActionResult Snapshot([FromQuery] string group) =>
        _catalog.HasGroup(group)
            ? Ok(new { group, values = _store.Snapshot(_catalog.TagsOf(group)) })
            : NotFound(new { error = $"нет группы {group}" });

    // GET /api/plc/read?tag=NonRetain_DB.meas.channels[2].t&tag=...
    [HttpGet("read")]
    public async Task<IActionResult> Read([FromQuery] string[] tag, CancellationToken ct)
    {
        if (tag.Length == 0) return BadRequest(new { error = "не указан tag" });
        var normalized = tag.Select(PlcTagCatalog.Normalize).ToList();
        var read = await _plc.ReadTagsAsync(normalized, ct);
        return Ok(read.Select(kv => new { tag = kv.Key, value = kv.Value.Value,
                                          good = kv.Value.Good, error = kv.Value.Error }));
    }

    public sealed record WriteDto(string Tag, JsonElement Value, int PulseMs = 0);

    [HttpPost("write")]
    public async Task<IActionResult> Write([FromBody] WriteDto dto, CancellationToken ct)
    {
        if (!_policy.KeyOk(Request.Headers["X-Api-Key"])) return Unauthorized();
        var tag = PlcTagCatalog.Normalize(dto.Tag);
        if (!_policy.IsAllowed(tag)) return StatusCode(403, new { error = "тег вне WriteAllow", tag });

        var id = Guid.NewGuid().ToString("N");
        await _queue.EnqueueAsync(new WriteJob(tag, dto.Value, id, "rest", dto.PulseMs,
                                              User.Identity?.Name), ct);
        return Accepted(new { requestId = id, tag });
    }

    [HttpGet("telemetry")]
    public IActionResult Telemetry()
    {
        var t = _store.Telemetry;
        return t is null ? NotFound(new { error = "пакетов ещё не было" })
                         : Ok(new { seq = t.Seq, ts = t.RxUtc, values = t.Values });
    }
}