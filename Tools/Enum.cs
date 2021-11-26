using System.Collections.Generic;
using System.Runtime.InteropServices;
using _DebuggerStepThrough = System.Diagnostics.DebuggerStepThroughAttribute;

namespace System
{
    [_DebuggerStepThrough]
    public static class Enum<T> where T : struct
    {
        [ComVisible(true)]
        public static string Format(object value, string format) => Enum.Format(typeof(T), value, format);
        [ComVisible(true)]
        public static string GetName(object value) => Enum.GetName(typeof(T), value);
        [ComVisible(true)]
        public static string[] GetNames() => Enum.GetNames(typeof(T));
        [ComVisible(true)]
        public static Type GetUnderlyingType() => Enum.GetUnderlyingType(typeof(T));
        [ComVisible(true)]
        public static T[] GetValues()
        {
            Array a = Enum.GetValues(typeof(T));
            T[] ret = new T[a.Length];
            a.CopyTo(ret, 0);
            return ret;
        }
        [ComVisible(true)]
        public static T[] GetValues(params T[] exclude)
        {
            List<T> ret = new List<T>();
            foreach (T n in Enum.GetValues(typeof(T)))
                if (!exclude.Contains(n))
                    ret.Add(n);
            return ret.ToArray();
        }
        [ComVisible(true)]
        public static bool IsDefined(object value) => Enum.IsDefined(typeof(T), value);

        [ComVisible(true)]
        public static bool TryParse(string value, out T result)
        {
            if (Enum<T>.IsDefined(value))
            {
                result = Enum<T>.Parse(value);
                return true;
            }
            result = default(T);
            return false;
        }

        public static bool TryParse(object value, out T result)
        {
            if (value != null)
            {
                if (value is string)
                    return Enum.TryParse((string)value, true, out result);
                Type t2 = value.GetType();
                try
                {
                    if (t2.IsPrimitive)
                    {
                        value = Convert.ChangeType(value, Enum.GetUnderlyingType(typeof(T)));
                        if (Enum.IsDefined(typeof(T), value))
                        {
                            result = (T)Enum.ToObject(typeof(T), value);
                            return true;
                        }
                    }
                }
                catch { }
            }
            //if (Enum<T>.IsDefined(value))
            //{
            //    result = Enum<T>.Parse(value);
            //    return true;
            //}
            result = default(T);
            return false;
        }

        [ComVisible(true)]
        public static T TryParse(string value)
        {
            if (Enum<T>.IsDefined(value))
                return Enum<T>.Parse(value);
            return default(T);
        }

        [ComVisible(true)]
        public static T Parse(string value) => (T)Enum.Parse(typeof(T), value);
        [ComVisible(true)]
        public static T Parse(string value, bool ignoreCase) => (T)Enum.Parse(typeof(T), value, ignoreCase);
        [ComVisible(true)]
        public static T ToObject(byte value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        public static T ToObject(int value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        public static T ToObject(long value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        public static T ToObject(object value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        //[CLSCompliant(false)]
        public static T ToObject(sbyte value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        public static T ToObject(short value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        //[CLSCompliant(false)]
        public static T ToObject(uint value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        //[CLSCompliant(false)]
        public static T ToObject(ulong value) => (T)Enum.ToObject(typeof(T), value);
        [ComVisible(true)]
        //[CLSCompliant(false)]
        public static T ToObject(ushort value) => (T)Enum.ToObject(typeof(T), value);
    }

    public static class _EnumExtensions
    {
        public static bool HasFlag(this Enum n,  params Enum[] flag)
        {
            for (int i = 0, j = flag.Length; i < j; i++)
                if (n.HasFlag(flag[i]))
                    return true;
            return false;
        }
    }
}
