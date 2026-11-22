using Microsoft.Extensions.Options;
using Siemens.Simatic.S7.Webserver.API.Services;
using Siemens.Simatic.S7.Webserver.API.Services.RequestHandling;
using SiemensPlcServer.Models;

namespace SiemensPlcServer.Services;

public sealed class PlcConnection : IAsyncDisposable
{
    private readonly PlcOptions _opt;
    private readonly ILogger<PlcConnection> _log;
    private readonly ApiStandardServiceFactory _factory = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    private IApiRequestHandler? _handler;
    private DateTime _lastUsedUtc = DateTime.MinValue;

    public PlcConnection(IOptions<PlcOptions> opt, ILogger<PlcConnection> log)
    {
        _opt = opt.Value;
        _log = log;
        if (_opt.AcceptInvalidCertificate)
            ServerCertificateCallback.CertificateCallback = (_, _, _, _) => true;
    }

    public IApiRequestFactory RequestFactory => _factory.GetApiRequestFactory();
    public bool IsConnected => _handler is not null;
    public string? LastError { get; private set; }
    public DateTime LastSuccessUtc { get; private set; }

    /// Единственная точка доступа к ПЛК: гейт + ленивый логин + ping + один retry.
    public async Task<T> ExecuteAsync<T>(
    Func<IApiRequestHandler, CancellationToken, Task<T>> action, CancellationToken ct)
{
    await _gate.WaitAsync(ct);
    try
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(TimeSpan.FromSeconds(_opt.RequestTimeoutSeconds));

                // ЛОГИН тоже под таймаутом: у HttpClient фабрики он зашит 10 минут
                var h = await EnsureConnectedAsync(timeout.Token);
                var result = await action(h, timeout.Token);

                _lastUsedUtc = LastSuccessUtc = DateTime.UtcNow;
                LastError = null;
                return result;
            }
            catch (Exception ex) when (attempt == 1 && !ct.IsCancellationRequested)
            {
                _log.LogWarning(ex, "ПЛК: сбой вызова, пересоздаю сессию");
                LastError = ex.Message;
                await DropSessionAsync();
                await Task.Delay(300, ct);
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                if (!ct.IsCancellationRequested) await DropSessionAsync();
                throw;
            }
        }
    }
    finally { _gate.Release(); }
}

    private async Task<IApiRequestHandler> EnsureConnectedAsync(CancellationToken ct)
    {
        if (_handler is not null)
        {
            // Сессия Web API удаляется CPU после 120 с без вызовов — держим её живой.
            if ((DateTime.UtcNow - _lastUsedUtc).TotalSeconds >= _opt.KeepAliveSeconds)
                await _handler.ApiPingAsync(ct);
            return _handler;
        }

        _log.LogInformation("ПЛК: Api.Login {User}@{Ip}", _opt.Username, _opt.IpAddress);
        _handler = await _factory.GetApiHttpClientRequestHandlerAsync(
            _opt.IpAddress, _opt.Username, _opt.Password, ct);
        _lastUsedUtc = DateTime.UtcNow;
        _log.LogInformation("ПЛК: сессия установлена");
        return _handler;
    }

    private async Task DropSessionAsync()
    {
        var h = _handler;
        _handler = null;
        if (h is null) return;
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await h.ApiLogoutAsync(cts.Token);   // освобождаем слот сессии на CPU
        }
        catch { /* ПЛК уже недоступен — слот освободится по таймауту */ }
    }

    public async ValueTask DisposeAsync() => await DropSessionAsync();
}