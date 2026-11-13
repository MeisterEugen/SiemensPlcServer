using System;
using Microsoft.AspNetCore.Mvc;
using SiemensPlcServer.Services;

namespace SiemensPlcServer.Controllers;
[ApiController]
[Route("api/[controller]")]
public class PlcController : ControllerBase
{
    private readonly PlcCommunicationService _plcService;

    public PlcController(PlcCommunicationService plcService)
    {
        _plcService = plcService;
    }
        // Пример запроса: GET /api/Plc/read?varName="DB1"."MyTag"
    [HttpGet("read")]
    public async Task<IActionResult> ReadVariable([FromQuery] string varName)
    {
        if (string.IsNullOrWhiteSpace(varName))
            return BadRequest("Имя переменной не указано");

        var result = await _plcService.ReadVariableAsync(varName);
        return Ok(new { variable = varName, value = result });
    }

    // Пример запроса: POST /api/Plc/write
    [HttpPost("write")]
    public async Task<IActionResult> WriteVariable([FromBody] WriteRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.VariableName))
            return BadRequest("Имя переменной не указано");

        var result = await _plcService.WriteVariableAsync(request.VariableName, request.Value);
        return Ok(new { variable = request.VariableName, result });
    }

}

public class WriteRequest
{
    public string VariableName { get; set; } = string.Empty;
    public object Value { get; set; } = null!; // Позволяет передать int, bool, string и т.д.
}
