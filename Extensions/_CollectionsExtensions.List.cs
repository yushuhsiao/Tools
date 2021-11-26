using System.Threading;

namespace System.Collections.Generic
{
    partial class _CollectionsExtensions
    {
        public static T Get<T>(this IList<T> list, int index)
        {
            if (list == null) return default(T);
            if (index < list.Count)
                return list[index];
            return default(T);
        }
        
        public static List<T> Convert<TSrc, T>(this List<TSrc> list, Func<TSrc, T> getValue)
        {
            if (list == null) return null;
            List<T> result = new List<T>();
            for (int i = 0, n = list.Count; i < n; i++)
                result.Add(getValue(list[i]));
            return result;
        }

        public static int RemoveAll<T>(this IList<T> list, T value)
        {
            int result = 0;
            while (list.Remove(value))
                result++;
            return result;
        }

        public static int RemoveWhen<T>(this IList<T> list, Predicate<T> match)
        {
            int count = 0;
            for (int i = list.Count - 1; i >= 0; i--)
            {
                if (match(list[i]))
                {
                    list.RemoveAt(i);
                    count++;
                }
            }
            return count;
        }

        public static bool Remove<T>(this IList<T> list, T item, object syncLock = null)
        {
            if (list == null) return false;
            int cnt;
            for (cnt = 0; list.Contains(item); cnt++)
                list.Remove(item);
            return cnt > 0;
        }

        public static bool AddOnce<T>(this IList<T> list, T item, object syncLock = null)
        {
            if (list == null) return false;
            if (list.Contains(item))
                return false;
            list.Add(item);
            return true;
        }

        public static T Add<T>(this IList<T> list, T item, object syncLock = null)
        {
            if (list == null) return item;
            list.Add(item);
            return item;
        }

        public static bool TryAdd<T>(this IList<T> list, T item, object syncLock = null)
        {
            if (list == null) return false;
            if (list.Contains(item))
                return false;
            list.Add(item);
            return true;
        }

        public static int TryAdd<T>(this IList<T> list, IEnumerable<T> items)
        {
            int cnt = 0;
            foreach (T item in items)
            {
                if (list.Contains(item)) continue;
                list.Add(item);
                cnt++;
            }
            return cnt;
        }

        public static bool AddWhen<T>(this IList<T> list, T value, Predicate<T> match)
        {
            for (int i = list.Count - 1; i >= 0; i--)
                if (match(list[i]))
                    return false;
            list.Add(value);
            return true;
        }

        public static List<T> Trim<T>(this List<T> list, bool nullOnEmpty = false)
        {
            if (list == null) return null;
            if (list.Count == 0 && nullOnEmpty)
                return null;
            return list;
        }

        public static IEnumerable<T> ForEach<T>(this IList<T> users, bool sync_lock = true)
        {
            if (sync_lock)
                Monitor.Enter(users);
            try
            {
                for (int i = users.Count - 1; i >= 0; i--)
                    yield return users[i];
            }
            finally
            {
                if (sync_lock)
                    Monitor.Exit(users);
            }
        }

        public static bool Contains(this IList<string> list, string item, bool ignoreCase)
        {
            if (ignoreCase)
            {
                for (int i = 0, n = list.Count; i < n; i++)
                    if (string.Compare(list[i], item, true) == 0)
                        return true;
                return false;
            }
            else
                return list.Contains(item);
        }

        public static IEnumerable<T> FindEach<T>(this IList<T> list, Predicate<T> match)
        {
            if ((list != null) && (match != null))
            {
                for (int i = 0, n = list.Count; i < n; i++)
                    if (match(list[i]))
                        yield return list[i];
            }
        }
    }
}