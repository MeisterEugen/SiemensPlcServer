using System;
using System.Threading.Tasks;
using Siemens.Simatic.S7.Webserver.API;
using Siemens.Simatic.S7.Webserver.API.Services.RequestHandling;
using Siemens.Simatic.S7.Webserver.API.Services;
using SiemensPlcServer.Models;
using SiemensPlcServer.Models.PlcSettings;

namespace SiemensPlcServer.Services;

public class PlcCommunicationService
{
    private readonly PlcSettings _settings;
    private ApiHttpClientRequestHandler? _requestHandler;
    private readonly ApiStandardServiceFactory _serviceFactory;

    public PlcCommunicationService(PlcSettings settings)
    {
        _settings = settings;
        _serviceFactory = new ApiStandardServiceFactory();
        // обход сертификата ssl 
        ServerCertificateCallback.CertificateCallback = (sender, cert, chain, sslPolicyErrors) => true;
    }

    private async Task<ApiHttpClientRequestHandler> GetHandlerAsync()
    {
        if (_requestHandler == null)
        {
            var handler = await _serviceFactory.GetApiHttpClientRequestHandlerAsync(
                _settings.IpAddress,
                _settings.Username,
                _settings.Password
            );
            _requestHandler = handler as ApiHttpClientRequestHandler;
        }
        return _requestHandler!;
    }

    public async Task<string> ReadVariableAsync(string variableName)
    {
        try
        {
            var handler = await GetHandlerAsync();
            var result = await handler.PlcProgramReadAsync<object>(variableName);
            return result.Result?.ToString() ?? "null";
        }
        catch (Exception ex)
        {
            return $"Ошибка чтения: {ex.Message}";
        }
    }


    public async Task<string> WriteVariableAsync(string variableName, object value)
    {
        try
        {
            var handler = await GetHandlerAsync();
            await handler.PlcProgramWriteAsync(variableName, value);
            return "Успешно записано";
        }
        catch (Exception ex)
        {
            return $"Ошибка записи: {ex.Message}";
        }
    }

    public async Task<Dictionary<string, object>> ReadMultipleVariablesAsync(IEnumerable<string> variableNames)
    {
        var handler = await GetHandlerAsync();
        var data = new Dictionary<string, object>();

        // 1. Создаем список задач (Task) для каждой переменной
        var readTasks = variableNames.Select(async varName =>
        {
            try
            {
                var result = await handler.PlcProgramReadAsync<object>(varName);
                // Возвращаем кортеж с именем, значением и флагом успеха
                return (Name: varName, Value: (object)result.Result, Success: true);
            }
            catch (Exception ex)
            {
                // Если одна переменная не прочиталась, мы не ломаем весь пакет, а просто записываем ошибку в значение
                return (Name: varName, Value: (object)$"Ошибка: {ex.Message}", Success: false);
            }
        });

        // 2. Выполняем все HTTP-запросы параллельно (намного быстрее, чем цикл foreach)
        var results = await Task.WhenAll(readTasks);

        // 3. Собираем результаты в словарь
        foreach (var item in results)
        {
            data[item.Name] = item.Value;
        }

        return data;
    }

}
