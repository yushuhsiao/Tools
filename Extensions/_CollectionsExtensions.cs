using _DebuggerStepThrough = System.Diagnostics.DebuggerStepThroughAttribute;

namespace System.Collections.Generic
{
    [_DebuggerStepThrough]
    public static partial class _CollectionsExtensions
    {
        public static T Find<T>(this T[] array, Predicate<T> match)
        {
            if (array != null)
                for (int i = 0, n = array.Length; i < n; i++)
                    if (match(array[i]))
                        return array[i];
            return default(T);
        }

        public static bool In<T>(this T? n, params T[] args) where T : struct
        {
            if (n.HasValue)
                return args.Contains(n.Value);
            return false;
        }
        public static bool In<T>(this T n, params T[] args) where T : struct
        {
            return args.Contains(n);
        }

        public static bool Contains<T>(this T[] src, T? value) where T : struct
        {
            if (value.HasValue)
                return src.Contains(value.Value);
            return false;
        }
        public static bool Contains<T>(this T[] src, T value)
        {
            if (src != null)
                for (int i = 0, n = src.Length; i < n; i++)
                    if (EqualityComparer<T>.Default.Equals(src[i], value))
                        return true;
            return false;
        }
        public static bool Contains(this Array src, object value)
        {
            if (src != null)
                foreach (object s in src)
                    if (s == value)
                        return true;
            return false;
        }
        public static int IndexOf<T>(this T[] src, T value) => Array.IndexOf<T>(src, value);

        public static bool TryGetValueAt<T>(this T[] array, int index, out T result)
        {
            if (index < array.Length)
            {
                result = array[index];
                return true;
            }
            result = default(T);
            return false;
        }

        public static T GetValueAt<T>(this T[] array, int index)
        {
            T result;
            array.TryGetValueAt(index, out result);
            return result;
        }

        public static int IndexOf(this byte[] src, byte[] value, int start, int count)
        {
            if ((src == null) || (value == null)) return -1;
            if (src.Length < value.Length) return -1;
            //int end = start + count;
            //if (src.Length < end)
            //    end = src.Length;
            //for (; start < end; start++)
            //{
            //    count = end - start - value.Length + 1;
            //    start = Array.IndexOf<byte>(src, value[0], start, count);
            //    bool f = true;
            //    count = value.Length;
            //    for (int i = start + 1, j = 1; f && (j < count); i++, j++)
            //        f = src[i] == value[j];
            //    if (f) return start;
            //}

            for (int i = start, end = start + count - value.Length; i <= end; i++)
            {
                if (src[i] == value[0])
                {
                    bool f = true;
                    int length = value.Length;
                    for (int j1 = i + 1, j2 = 1; f && (j2 < length); j1++, j2++)
                        f = src[j1] == value[j2];
                    if (f) return i;
                }
            }
            return -1;
        }

        public static bool IsNullOrEmpty(this IList list)
        {
            if (list == null) return true;
            if (list.Count == 0) return true;
            return false;
        }

        public static T[] Add<T>(this T[] array, T item)
        {
            int len = array.Length;
            Array.Resize(ref array, len + 1);
            array[len] = item;
            return array;
        }

        public static TValue GetValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key)
        {
            if (dict.TryGetValue(key, out TValue value))
                return value;
            return default;
        }
    }

    //    [_DebuggerStepThrough]
    //    public static class Extensions
    //    {
    //        public static void AddNoDuplicate<T>(this List<T> list, T item)
    //        {
    //            if (list != null)
    //                if (!list.Contains(item))
    //                    list.Add(item);
    //        }

    //        public static void RemoveAt<T>(this List<T> list, T item)
    //        {
    //            if (list != null)
    //                while (list.Remove(item)) { }
    //        }
    //    }
}