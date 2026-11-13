using Microsoft.AspNetCore.SignalR;
using SiemensPlcServer.Hubs;
using SiemensPlcServer.Services;

namespace SiemensPlcServer.BackgroundServices;

public class PlcMonitoringService : BackgroundService
{
    private readonly IHubContext<PlcHub> _hubContext;
    private readonly PlcCommunicationService _plcService;
    private readonly ILogger<PlcMonitoringService> _logger;

    // Список переменных, которые нужно мониторить
    private readonly Dictionary<string, object> _monitoredVariables = new()
    {
        { "\"DB1\".\"Temperature\"", 0 },
        { "\"DB1\".\"MotorSpeed\"", 0 },
        { "\"DB1\".\"Status\"", "" }
    };

    public PlcMonitoringService(
        IHubContext<PlcHub> hubContext,
        PlcCommunicationService plcService,
        ILogger<PlcMonitoringService> logger)
    {
        _hubContext = hubContext;
        _plcService = plcService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Сервис мониторинга ПЛК запущен");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Читаем все мониторимые переменные
                foreach (var variableName in _monitoredVariables.Keys.ToList())
                {
                    var result = await _plcService.ReadVariableAsync(variableName);
                    
                    // Если значение изменилось или это первое чтение
                    if (!_monitoredVariables.ContainsKey(variableName) || 
                        _monitoredVariables[variableName]?.ToString() != result)
                    {
                        _monitoredVariables[variableName] = result!;
                        
                        // Отправляем обновление всем подключенным клиентам
                        await _hubContext.Clients.All.SendAsync(
                            "ReceivePlcData", 
                            new { variable = variableName, value = result }
                        );
                        
                        _logger.LogInformation($"Обновлено: {variableName} = {result}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при чтении с ПЛК");
                
                // Отправляем ошибку всем клиентам
                await _hubContext.Clients.All.SendAsync(
                    "ReceiveError", 
                    new { error = ex.Message, timestamp = DateTime.Now }
                );
            }

            // Интервал опроса (1 секунда)
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}