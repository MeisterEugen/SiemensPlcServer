namespace SiemensPlcServer.Models;

public sealed class PlcOptions
{
    public const string Section = "Plc";
    public string IpAddress { get; set; } = "192.168.1.215";
    public string Username { get; set; } = "admin";
    public string Password { get; set; } = "";
    public bool AcceptInvalidCertificate { get; set; } = true;
    public int RequestTimeoutSeconds { get; set; } = 10;
    public int KeepAliveSeconds { get; set; } = 30;
    public int MaxTagsPerBulk { get; set; } = 200;
    public int MaxExpandTags { get; set; } = 400;
    public string WriteApiKey { get; set; } = "";
    public List<string> WriteAllow { get; set; } = new();
    public List<PollGroupOptions> Groups { get; set; } = new();
}

public sealed class PollGroupOptions
{
    public string Name { get; set; } = "";
    public int PeriodMs { get; set; } = 1000;
    public bool Enabled { get; set; } = true;
    public List<string> Tags { get; set; } = new();
}

public sealed class TelemetryOptions
{
    public const string Section = "Telemetry";
    public bool Enabled { get; set; } = true;
    public string BindAddress { get; set; } = "0.0.0.0";
    public int Port { get; set; } = 50100;
    public int ExpectedLength { get; set; } = 512;
    public string LayoutFile { get; set; } = "Config/telemetry.layout.json";
    public int StaleAfterMs { get; set; } = 1000;
    public int BroadcastPeriodMs { get; set; } = 100;
    public bool ConfigurePlcOnStartup { get; set; } = true;
    public string ServerIp { get; set; } = "192.168.1.100";
    public int PlcInterface { get; set; } = 2;
    public int PeriodMs { get; set; } = 100;
}