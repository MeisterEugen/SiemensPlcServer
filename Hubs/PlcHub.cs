using System;
using Microsoft.AspNetCore.SignalR;
namespace SiemensPlcServer.Hubs;

public class PlcHub : Hub
{
    public override async Task OnConnectedAsync()
    {
        Console.WriteLine($"Клиент подключён");
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        Console.WriteLine($"Клиент подключён");
        await base.OnDisconnectedAsync(exception);
    }

    public async Task RequestVariableUpdate(string variableName)
    {
        Console.WriteLine($"Клиент запросил обновление: {variableName}");
        // сервис чтения
    }
}
