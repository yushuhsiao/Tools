using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

namespace System
{
    [DebuggerStepThrough]
    public static class Empty<T>
    {
        public static readonly T[] Array = new T[0];
        //public static readonly IEnumerable<T> List = new List<T>();
        //public static readonly T Object = (T)Activator.CreateInstance(typeof(T));
    }
    //[DebuggerStepThrough]
    //public static class _Empty
    //{
    //    public static readonly string[] StringArray = _Empty<string>.Array;

    //    public static bool IsEmpty<T>(this IEnumerable<T> list)
    //    {
    //        foreach (var n in list)
    //            return false;
    //        return true;
    //    }

    //    public static IEnumerable<T> Empty<T>(this IEnumerable<T> src)
    //        => System.Empty<T>.List;

    //    public static IEnumerable<T> NullToEmpty<T>(this IEnumerable<T> src)
    //        => src ?? System.Empty<T>.List;

    //    public static T NullToEmpty<T>(this T src) where T : class
    //        => src ?? System.Empty<T>.Object;
    //}
}
