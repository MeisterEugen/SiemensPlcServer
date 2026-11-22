namespace SiemensPlcServer.Services;

public readonly record struct PlcEventDef(ushort Code, bool BothEdges);

public static class PlcEventCatalog
{
    private static readonly Dictionary<string, PlcEventDef> Map = new(StringComparer.Ordinal)
    {
        // HV и затворы — состояние
        ["hv_on_1"]              = new(0x2101, true),
        ["hv_on_2"]              = new(0x2102, true),
        ["hv_on_3"]              = new(0x2103, true),
        ["gate_open_1"]          = new(0x2104, true),
        ["gate_open_2"]          = new(0x2105, true),
        ["gate_open_3"]          = new(0x2106, true),
        ["gate_closed_1"]        = new(0x2107, true),
        ["gate_closed_2"]        = new(0x2108, true),
        ["gate_closed_3"]        = new(0x2109, true),

        // Кнопки ЛПУ — импульсные, продлены ПЛК на 2 с
        ["lpc_hv_on_1"]          = new(0x210A, false),
        ["lpc_hv_on_2"]          = new(0x210B, false),
        ["lpc_hv_on_3"]          = new(0x210C, false),
        ["lpc_hv_off_1"]         = new(0x210D, false),
        ["lpc_hv_off_2"]         = new(0x210E, false),
        ["lpc_hv_off_3"]         = new(0x210F, false),
        ["lpc_gate_open_1"]      = new(0x2110, false),
        ["lpc_gate_open_2"]      = new(0x2111, false),
        ["lpc_gate_open_3"]      = new(0x2112, false),
        ["lpc_gate_close_1"]     = new(0x2113, false),
        ["lpc_gate_close_2"]     = new(0x2114, false),
        ["lpc_gate_close_3"]     = new(0x2115, false),

        ["lpc_abort"]            = new(0x2401, true),   // сырое, 1→0 = отпущен

        ["frame_at_measure"]     = new(0x2501, true),
        ["frame_at_parking"]     = new(0x2502, true),
        ["frame_to_measure"]     = new(0x2503, true),
        ["frame_to_parking"]     = new(0x2504, true),
        ["lpc_frame_to_measure"] = new(0x2505, false),
        ["lpc_frame_to_parking"] = new(0x2506, false),
        ["lpc_jog_forward"]      = new(0x2507, false),
        ["lpc_jog_backward"]     = new(0x2508, false),

        ["standartisation"]      = new(0x2801, true),
        ["single_meas"]          = new(0x2802, true),
        ["strip"]                = new(0x2803, true),
    };

    public static bool TryGet(string field, out PlcEventDef def) => Map.TryGetValue(field, out def);
}