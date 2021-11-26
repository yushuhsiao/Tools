using UInt08 = System.Byte;
using SInt08 = System.SByte;
using SInt16 = System.Int16;
using SInt32 = System.Int32;
using SInt64 = System.Int64;

namespace System
{
    public static class _MathExtensions
    {
        public static SInt08? Min(this SInt08? val1, SInt08? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt08? Min(this UInt08? val1, UInt08? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt16? Min(this SInt16? val1, SInt16? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt16? Min(this UInt16? val1, UInt16? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt32? Min(this SInt32? val1, SInt32? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt32? Min(this UInt32? val1, UInt32? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt64? Min(this SInt64? val1, SInt64? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt64? Min(this UInt64? val1, UInt64? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static Single? Min(this Single? val1, Single? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static Double? Min(this Double? val1, Double? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static Decimal? Min(this Decimal? val1, Decimal? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }

        public static SInt08? Max(this SInt08? val1, SInt08? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt08? Max(this UInt08? val1, UInt08? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt16? Max(this SInt16? val1, SInt16? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt16? Max(this UInt16? val1, UInt16? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt32? Max(this SInt32? val1, SInt32? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt32? Max(this UInt32? val1, UInt32? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt64? Max(this SInt64? val1, SInt64? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt64? Max(this UInt64? val1, UInt64? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static Single? Max(this Single? val1, Single? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static Double? Max(this Double? val1, Double? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static Decimal? Max(this Decimal? val1, Decimal? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }

        public static SInt08 Min(this SInt08 value, SInt08 compare) => Math.Min(value, compare);
        public static UInt08 Min(this UInt08 value, UInt08 compare) => Math.Min(value, compare);
        public static SInt16 Min(this SInt16 value, SInt16 compare) => Math.Min(value, compare);
        public static UInt16 Min(this UInt16 value, UInt16 compare) => Math.Min(value, compare);
        public static SInt32 Min(this SInt32 value, SInt32 compare) => Math.Min(value, compare);
        public static UInt32 Min(this UInt32 value, UInt32 compare) => Math.Min(value, compare);
        public static SInt64 Min(this SInt64 value, SInt64 compare) => Math.Min(value, compare);
        public static UInt64 Min(this UInt64 value, UInt64 compare) => Math.Min(value, compare);
        public static Single Min(this Single value, Single compare) => Math.Min(value, compare);
        public static Double Min(this Double value, Double compare) => Math.Min(value, compare);
        public static Decimal Min(this Decimal value, Decimal compare) => Math.Min(value, compare);

        public static SInt08 Max(this SInt08 value, SInt08 compare) => Math.Max(value, compare);
        public static UInt08 Max(this UInt08 value, UInt08 compare) => Math.Max(value, compare);
        public static SInt16 Max(this SInt16 value, SInt16 compare) => Math.Max(value, compare);
        public static UInt16 Max(this UInt16 value, UInt16 compare) => Math.Max(value, compare);
        public static SInt32 Max(this SInt32 value, SInt32 compare) => Math.Max(value, compare);
        public static UInt32 Max(this UInt32 value, UInt32 compare) => Math.Max(value, compare);
        public static SInt64 Max(this SInt64 value, SInt64 compare) => Math.Max(value, compare);
        public static UInt64 Max(this UInt64 value, UInt64 compare) => Math.Max(value, compare);
        public static Single Max(this Single value, Single compare) => Math.Max(value, compare);
        public static Double Max(this Double value, Double compare) => Math.Max(value, compare);
        public static Decimal Max(this Decimal value, Decimal compare) => Math.Max(value, compare);

        public static bool IsBetWeens(this SInt08 value, SInt08 lowerValue, SInt08 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this UInt08 value, UInt08 lowerValue, UInt08 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this SInt16 value, SInt16 lowerValue, SInt16 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this UInt16 value, UInt16 lowerValue, UInt16 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this SInt32 value, SInt32 lowerValue, SInt32 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this UInt32 value, UInt32 lowerValue, UInt32 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this SInt64 value, SInt64 lowerValue, SInt64 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this UInt64 value, UInt64 lowerValue, UInt64 upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this Single value, Single lowerValue, Single upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this Double value, Double lowerValue, Double upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
        public static bool IsBetWeens(this Decimal value, Decimal lowerValue, Decimal upperValue) { if (lowerValue > upperValue) return value >= upperValue && value <= lowerValue; else return value >= lowerValue && value <= upperValue; }
    }
}