using System.Threading;
using System.Threading.Tasks;

namespace System.Collections.Generic
{
    public class SyncList<T>
    {
        private bool useCache;
        private List<T> list1 = new List<T>();
        private T[] list2 = new T[0];

        public T[] Cache => Interlocked.CompareExchange(ref list2, null, null);

        public SyncList(bool useCache = true)
        {
            this.useCache = useCache;
        }

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
                if (useCache) Interlocked.Exchange(ref list2, list1.ToArray());
            }
            return item;
        }

        public T Add(T item, Func<T, Task> cb)
        {
            this.Add(item);
            if (cb != null) this.RunQueue(cb);
            return item;
        }

        public bool TryGetFirst(out T result, bool remove = false)
        {
            if (Monitor.TryEnter(list1))
            {
                try
                {
                    if (list1.Count > 0)
                    {
                        result = list1[0];
                        if (remove)
                        {
                            list1.RemoveAt(0);
                            if (useCache) Interlocked.Exchange(ref list2, list1.ToArray());
                        }
                        return true;
                    }
                }
                finally { Monitor.Exit(list1); }
            }
            result = default;
            return false;
        }

        public bool Remove(T item)
        {
            bool ret = false;
            lock (list1)
            {
                while (list1.Remove(item))
                    ret = true;
                if (useCache) Interlocked.Exchange(ref list2, list1.ToArray());
            }
            return ret;
        }

        public void Clear()
        {
            lock (list1)
            {
                list1.Clear();
                Interlocked.Exchange(ref list2, list1.ToArray());
            }
        }


        private object _runQueue;

        public void RunQueue(Func<T, Task> cb)
        {
            if (Interlocked.CompareExchange(ref _runQueue, this, null) != null) return;
            var task = Task.Run(async () =>
            {
                try
                {
                    for (int n1 = 0; n1 < 100; n1++)
                    {
                        for (int n2 = 0; n2 < 100; n2++)
                        {
                            if (this.TryGetFirst(out var item, true))
                            {
                                n1 = n2 = 0;
                                await cb(item);
                            }
                            else
                            {
                                await Task.Delay(n1 == 0 ? 10 : 100);
                            }
                        }
                    }
                }
                finally { Interlocked.Exchange(ref _runQueue, null); }
            });
        }

        //public async Task RunQueue(Action<T> cb)
        //{
        //    if (Interlocked.CompareExchange(ref _runQueue, this, null) != null) return;
        //    try
        //    {
        //        for (int i = 0; i < 10; i++)
        //        {
        //            if (this.TryGetFirst(out var item, true))
        //            {
        //                i = 0;
        //                cb(item);
        //            }
        //            else
        //            {
        //                await Task.Delay(10);
        //            }
        //        }
        //    }
        //    finally { Interlocked.Exchange(ref _runQueue, null); }
        //}
    }
}