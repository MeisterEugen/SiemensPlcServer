using SiemensPlcServer.Models.PlcSettings;
using SiemensPlcServer.Services;
using SiemensPlcServer.Hubs;
using SiemensPlcServer.BackgroundServices;
var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
//builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
//signalR
builder.Services.AddSignalR();
// Привязка настроек
var plcSettings = new PlcSettings();
builder.Configuration.GetSection("PlcSettings").Bind(plcSettings);
builder.Services.AddSingleton(plcSettings);
builder.Services.AddSingleton<PlcCommunicationService>();
var app = builder.Build();





// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
        app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Siemens PLC API V1");
        c.RoutePrefix = string.Empty; 
    });
}

//app.UseHttpsRedirection();

app.UseRouting();

app.UseAuthorization();

app.MapControllers();

// Маршрут для SignalR Hub
app.MapHub<PlcHub>("/plchub");

app.Run();
