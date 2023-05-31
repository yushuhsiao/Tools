using System.Threading;

namespace System.Collections.Generic
{
    public class SyncList<T>
    {
        private List<T> list1 = new List<T>();
        private T[] list2 = new T[0];

        public T[] Cache => Interlocked.CompareExchange(ref list2, null, null);

        public T this[int index]
        {
            get
            {
                var c = this.Cache;
                if (index < c.Length)
                    return c[index];
                return default;
            }
        }

        public T Add(T item)
        {
            lock (list1)
            {
                list1.Add(item);
                Interlocked.Exchange(ref list2, list1.ToArray());
            }
            return item;
        }

        public bool Remove(T item)
        {
            bool ret = false;
            lock (list1)
            {
                while (list1.Remove(item))
                    ret = true;
                Interlocked.Exchange(ref list2, list1.ToArray());
            }
            return ret;
        }
    }
}