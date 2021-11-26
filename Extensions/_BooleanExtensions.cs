using System.Diagnostics;
using System.Threading;

namespace System
{
    [DebuggerStepThrough]
    public static class _BooleanExtensions
    {
        public static Boolean ToBoolean(this Int32 n) { return n != 0; }
        public static Boolean ToBoolean(this Int64 n) { return n != 0; }
        public static Boolean ToBoolean(this Int32 n, ref Int32 location) => Interlocked.CompareExchange(ref location, 0, 0).ToBoolean();
        public static Boolean ToBoolean(this Int64 n, ref Int64 location) => Interlocked.CompareExchange(ref location, 0, 0).ToBoolean();

        public static Int32 ToInt32(this bool n, Int32 trueValue = 1) => n.ToInt32(() => 1);
        public static Int64 ToInt64(this bool n, Int64 trueValue = 1) => n.ToInt64(() => 1);
        public static Int32 ToInt32(this bool n, Func<Int32> trueValue) { if (n == false) return 0; Int32 nn = trueValue(); return nn == 0 ? 1 : nn; }
        public static Int64 ToInt64(this bool n, Func<Int64> trueValue) { if (n == false) return 0; Int64 nn = trueValue(); return nn == 0 ? 1 : nn; }

        public static Boolean ToInt32(this bool n, ref Int32 location, Int32 trueValue = 1) => Interlocked.Exchange(ref location, n.ToInt32(trueValue)).ToBoolean();
        public static Boolean ToInt64(this bool n, ref Int64 location, Int64 trueValue = 1) => Interlocked.Exchange(ref location, n.ToInt64(trueValue)).ToBoolean();
        public static Boolean ToInt32(this bool n, ref Int32 location, Func<Int32> trueValue) => Interlocked.Exchange(ref location, n.ToInt32(trueValue)).ToBoolean();
        public static Boolean ToInt64(this bool n, ref Int64 location, Func<Int64> trueValue) => Interlocked.Exchange(ref location, n.ToInt64(trueValue)).ToBoolean();
    }
}
