using System.Globalization;
using System.Text.Json;

namespace SiemensPlcServer.Services;

/// <summary>
/// JsonElement -> CLR-тип, который понимает PlcProgram.Write.
/// REAL => float (32 бита), LREAL => double. INT в S7 — 16 бит (short).
/// Тип берётся из PlcProgram.Browse; без него — консервативная эвристика.
/// </summary>
public static class PlcValueConverter
{
    public static object FromJson(JsonElement value, string? plcDataType)
    {
        var t = (plcDataType ?? "").Trim().ToLowerInvariant().Replace("_", "").Replace(" ", "");

        return t switch
        {
            "bool"            => AsBool(value),
            "sint"            => (sbyte)AsInt(value, sbyte.MinValue, sbyte.MaxValue, t),
            "byte" or "usint" => (byte)AsInt(value, byte.MinValue, byte.MaxValue, t),
            "int"             => (short)AsInt(value, short.MinValue, short.MaxValue, t),
            "uint" or "word"  => (ushort)AsInt(value, ushort.MinValue, ushort.MaxValue, t),
            "dint"            => (int)AsInt(value, int.MinValue, int.MaxValue, t),
            "udint" or "dword"=> (uint)AsInt(value, uint.MinValue, uint.MaxValue, t),
            "lint"            => AsInt(value, long.MinValue, long.MaxValue, t),
            "ulint" or "lword"=> AsUInt(value),

            "real"            => AsFloat(value),    // ← float, НЕ double
            "lreal"           => AsDouble(value),   // единственное место с double

            "char" or "wchar" => AsChar(value),
            "string" or "wstring" => AsString(value),

            ""                => Heuristic(value),
            _ => throw new NotSupportedException(
                     $"Тип ПЛК '{plcDataType}' пока не поддержан для записи " +
                     "(TIME/DT/DTL/S5TIME требуют отдельного конвертера)")
        };
    }

    // ── REAL: 32 бита ────────────────────────────────────────────────────────────
    private static float AsFloat(JsonElement v)
    {
        var d = AsDouble(v);
        if (double.IsNaN(d) || double.IsInfinity(d))
            throw new InvalidCastException("REAL: NaN/Infinity записать нельзя (в JSON их нет)");
        if (Math.Abs(d) > float.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(v), $"REAL: {d} вне ±{float.MaxValue:E}");
        return (float)d;
    }

    private static double AsDouble(JsonElement v) => v.ValueKind switch
    {
        JsonValueKind.Number => v.GetDouble(),
        JsonValueKind.String => double.Parse(v.GetString()!, NumberStyles.Float, CultureInfo.InvariantCulture),
        JsonValueKind.True   => 1,
        JsonValueKind.False  => 0,
        _ => throw new InvalidCastException($"Ожидалось число, получен {v.ValueKind}")
    };

    // ── Целые: точная проверка диапазона, без молчаливого переполнения ───────────
    private static long AsInt(JsonElement v, long min, long max, string type)
    {
        if (v.ValueKind == JsonValueKind.Number && v.TryGetInt64(out var exact))
            return InRange(exact, min, max, type);

        var d = AsDouble(v);
        if (double.IsNaN(d) || double.IsInfinity(d) || Math.Truncate(d) != d)
            throw new InvalidCastException($"{type}: ожидалось целое, получено {d}");
        if (d < min || d > max)
            throw new ArgumentOutOfRangeException(nameof(v), $"{type}: {d} вне [{min}..{max}]");
        return (long)d;
    }

    private static ulong AsUInt(JsonElement v)
    {
        if (v.ValueKind == JsonValueKind.Number && v.TryGetUInt64(out var u)) return u;
        var d = AsDouble(v);
        if (d < 0 || d > ulong.MaxValue || Math.Truncate(d) != d)
            throw new ArgumentOutOfRangeException(nameof(v), $"ULINT/LWORD: некорректное {d}");
        return (ulong)d;
    }

    private static long InRange(long x, long min, long max, string type) =>
        x < min || x > max
            ? throw new ArgumentOutOfRangeException(nameof(x), $"{type}: {x} вне [{min}..{max}]")
            : x;

    private static bool AsBool(JsonElement v) => v.ValueKind switch
    {
        JsonValueKind.True   => true,
        JsonValueKind.False  => false,
        JsonValueKind.Number => v.GetDouble() != 0,
        JsonValueKind.String => bool.TryParse(v.GetString(), out var b)
                                ? b : throw new InvalidCastException($"BOOL: '{v.GetString()}'"),
        _ => throw new InvalidCastException($"BOOL: получен {v.ValueKind}")
    };

    private static char AsChar(JsonElement v)
    {
        var s = AsString(v);
        return s.Length == 1 ? s[0] : throw new InvalidCastException("CHAR: ровно один символ");
    }

    private static string AsString(JsonElement v) =>
        v.ValueKind == JsonValueKind.String ? v.GetString()! : v.ToString();

    // ── Тип неизвестен: дробное считаем REAL (самый частый writable дробный) ─────
    private static object Heuristic(JsonElement v) => v.ValueKind switch
    {
        JsonValueKind.True or JsonValueKind.False => v.GetBoolean(),
        JsonValueKind.String => v.GetString()!,
        JsonValueKind.Number when v.TryGetInt64(out var l) =>
            l is >= short.MinValue and <= short.MaxValue ? (short)l :
            l is >= int.MinValue   and <= int.MaxValue   ? (int)l   : l,
        JsonValueKind.Number => (float)v.GetDouble(),
        _ => throw new InvalidCastException($"Не удалось привести {v.ValueKind}")
    };
}