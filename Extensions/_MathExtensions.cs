using UInt08_ = System.Byte;
using SInt08_ = System.SByte;
using UInt16_ = System.UInt16;
using SInt16_ = System.Int16;
using UInt32_ = System.UInt32;
using SInt32_ = System.Int32;
using UInt64_ = System.UInt64;
using SInt64_ = System.Int64;
using Single_ = System.Single;
using Double_ = System.Double;

namespace System
{
    public static class _MathExtensions
    {
        public static SInt08_? Min(this SInt08_? val1, SInt08_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt08_? Min(this UInt08_? val1, UInt08_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt16_? Min(this SInt16_? val1, SInt16_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt16_? Min(this UInt16_? val1, UInt16_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt32_? Min(this SInt32_? val1, SInt32_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt32_? Min(this UInt32_? val1, UInt32_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt64_? Min(this SInt64_? val1, SInt64_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt64_? Min(this UInt64_? val1, UInt64_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static Single_? Min(this Single_? val1, Single_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static Double_? Min(this Double_? val1, Double_? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }
        public static Decimal? Min(this Decimal? val1, Decimal? val2) { if (val1.HasValue && val2.HasValue) return Math.Min(val1.Value, val2.Value); return val1 ?? val2; }

        public static SInt08_? Max(this SInt08_? val1, SInt08_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt08_? Max(this UInt08_? val1, UInt08_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt16_? Max(this SInt16_? val1, SInt16_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt16_? Max(this UInt16_? val1, UInt16_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt32_? Max(this SInt32_? val1, SInt32_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt32_? Max(this UInt32_? val1, UInt32_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static SInt64_? Max(this SInt64_? val1, SInt64_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static UInt64_? Max(this UInt64_? val1, UInt64_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static Single_? Max(this Single_? val1, Single_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static Double_? Max(this Double_? val1, Double_? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }
        public static Decimal? Max(this Decimal? val1, Decimal? val2) { if (val1.HasValue && val2.HasValue) return Math.Max(val1.Value, val2.Value); return val1 ?? val2; }

        public static SInt08_ Min(this SInt08_ value, SInt08_ compare) => Math.Min(value, compare);
        public static UInt08_ Min(this UInt08_ value, UInt08_ compare) => Math.Min(value, compare);
        public static SInt16_ Min(this SInt16_ value, SInt16_ compare) => Math.Min(value, compare);
        public static UInt16_ Min(this UInt16_ value, UInt16_ compare) => Math.Min(value, compare);
        public static SInt32_ Min(this SInt32_ value, SInt32_ compare) => Math.Min(value, compare);
        public static UInt32_ Min(this UInt32_ value, UInt32_ compare) => Math.Min(value, compare);
        public static SInt64_ Min(this SInt64_ value, SInt64_ compare) => Math.Min(value, compare);
        public static UInt64_ Min(this UInt64_ value, UInt64_ compare) => Math.Min(value, compare);
        public static Single_ Min(this Single_ value, Single_ compare) => Math.Min(value, compare);
        public static Double_ Min(this Double_ value, Double_ compare) => Math.Min(value, compare);
        public static Decimal Min(this Decimal value, Decimal compare) => Math.Min(value, compare);

        public static SInt08_ Max(this SInt08_ value, SInt08_ compare) => Math.Max(value, compare);
        public static UInt08_ Max(this UInt08_ value, UInt08_ compare) => Math.Max(value, compare);
        public static SInt16_ Max(this SInt16_ value, SInt16_ compare) => Math.Max(value, compare);
        public static UInt16_ Max(this UInt16_ value, UInt16_ compare) => Math.Max(value, compare);
        public static SInt32_ Max(this SInt32_ value, SInt32_ compare) => Math.Max(value, compare);
        public static UInt32_ Max(this UInt32_ value, UInt32_ compare) => Math.Max(value, compare);
        public static SInt64_ Max(this SInt64_ value, SInt64_ compare) => Math.Max(value, compare);
        public static UInt64_ Max(this UInt64_ value, UInt64_ compare) => Math.Max(value, compare);
        public static Single_ Max(this Single_ value, Single_ compare) => Math.Max(value, compare);
        public static Double_ Max(this Double_ value, Double_ compare) => Math.Max(value, compare);
        public static Decimal Max(this Decimal value, Decimal compare) => Math.Max(value, compare);

        public static bool IsBetWeens(this SInt08_ value, SInt08_ lowerValue, SInt08_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this UInt08_ value, UInt08_ lowerValue, UInt08_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this SInt16_ value, SInt16_ lowerValue, SInt16_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this UInt16_ value, UInt16_ lowerValue, UInt16_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this SInt32_ value, SInt32_ lowerValue, SInt32_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this UInt32_ value, UInt32_ lowerValue, UInt32_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this SInt64_ value, SInt64_ lowerValue, SInt64_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this UInt64_ value, UInt64_ lowerValue, UInt64_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this Single_ value, Single_ lowerValue, Single_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this Double_ value, Double_ lowerValue, Double_ upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
        public static bool IsBetWeens(this Decimal value, Decimal lowerValue, Decimal upperValue, bool containLower = true, bool containUpper = true) { if (lowerValue > upperValue) return IsBetWeens(value, upperValue, lowerValue, containLower, containUpper); return (value > lowerValue && value < upperValue) || (containLower && (value == lowerValue)) || (containUpper && (value == upperValue)); }
    }
}