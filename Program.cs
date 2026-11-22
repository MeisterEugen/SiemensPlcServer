using Microsoft.Extensions.Options;
using SiemensPlcServer.BackgroundServices;
using SiemensPlcServer.Hubs;
using SiemensPlcServer.Models;
using SiemensPlcServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<PlcOptions>(builder.Configuration.GetSection(PlcOptions.Section));
builder.Services.Configure<TelemetryOptions>(builder.Configuration.GetSection(TelemetryOptions.Section));

// UDP-раскладка пакетов
builder.Services.AddSingleton(sp =>
{
    var o = sp.GetRequiredService<IOptions<TelemetryOptions>>().Value;
    var env = sp.GetRequiredService<IHostEnvironment>();
    return TelemetryLayout.Load(Path.Combine(env.ContentRootPath, o.LayoutFile),
                                sp.GetRequiredService<ILogger<TelemetryLayout>>());
});

// ПЛК сервисы
builder.Services.AddSingleton<PlcConnection>();
builder.Services.AddSingleton<PlcCommunicationService>();
builder.Services.AddSingleton<PlcTagCatalog>();
builder.Services.AddSingleton<PlcDataStore>();
builder.Services.AddSingleton<PlcWritePolicy>();
builder.Services.AddSingleton<PlcWriteQueue>();
builder.Services.AddSingleton<PlcTypeResolver>();

// Фоновые сервисы
builder.Services.AddHostedService<UdpTelemetryReceiver>();
builder.Services.AddHostedService<PlcWriteWorker>();
builder.Services.AddHostedService<PlcMonitoringService>(); 

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSignalR(o =>
{
    o.MaximumReceiveMessageSize = 64 * 1024;
    o.EnableDetailedErrors = builder.Environment.IsDevelopment();
    o.KeepAliveInterval = TimeSpan.FromSeconds(10);
    o.ClientTimeoutInterval = TimeSpan.FromSeconds(30);
});

var origins = builder.Configuration.GetSection("Cors:Origins").Get<string[]>() ?? Array.Empty<string>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
    .WithOrigins(origins)
    .AllowAnyHeader().AllowAnyMethod()
    .AllowCredentials()));   

var app = builder.Build();
// ФАЗА 1 каталога: синхронно, ПЛК не нужен. Группы существуют с первой миллисекунды.
app.Services.GetRequiredService<PlcTagCatalog>().BuildOffline();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Siemens PLC API V1");
        c.RoutePrefix = "swagger";
    });
}

app.UseCors();
app.UseDefaultFiles();
app.UseStaticFiles();       // wwwroot/index.html 
app.UseRouting();
app.UseAuthorization();

app.MapControllers();
app.MapHub<PlcHub>("/plchub");
app.MapGet("/health", (PlcDataStore s, PlcCommunicationService p,
                       IOptions<TelemetryOptions> t) =>
    Results.Ok(s.Status(p.IsConnected, p.LastError, t.Value.StaleAfterMs)));

app.Run();