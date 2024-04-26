using System.Threading;
using System.Threading.Tasks;

namespace System.Collections.Generic
{
    public class SyncList<T>
    {
        private bool useCache;
        private List<T> list1 = new List<T>();
        private readonly Interlocked<T[]> list2 = new Interlocked<T[]>() { Value = Array.Empty<T>() };

        public T[] Cache => list2.Value;

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
                if (useCache) list2.Value = list1.ToArray();
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
                            if (useCache) list2.Value = list1.ToArray();
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
                if (useCache) list2.Value = list1.ToArray();
            }
            return ret;
        }

        public void Clear()
        {
            lock (list1)
            {
                list1.Clear();
                if (useCache) list2.Value = list1.ToArray();
            }
        }



        public void RunQueue(Func<T, Task> cb)
        {
            _runQueue_Func.Value = cb;
            Generic.RunQueue.Add(this.RunQueue);
        }

        private readonly Interlocked<Func<T, Task>> _runQueue_Func = new Interlocked<Func<T, Task>>();
        public BusyState Busy { get; } = new BusyState();
        private async Task RunQueue_Proc()
        {
            var cb = _runQueue_Func.Value;
            if (cb == null) return;
            Console.WriteLine($"RunQueue : {typeof(T).FullName}");
            using (Busy.Enter(out var busy))
            {
                if (busy) return;
                int n;
                for (n = 0; this.TryGetFirst(out var item, true); n++)
                    await cb(item);
                Console.WriteLine($"RunQueue : {typeof(T).FullName}, {n}");
            }
        }

        private bool RunQueue()
        {
            if (_runQueue_Func.IsNull) return false;
            if (Busy.IsNotBusy && this.TryGetFirst(out var item, false))
                Task.Run(RunQueue_Proc);
            return true;
        }

        public static event Func<bool> Tick
        {
            add { Generic.RunQueue.Add(value); }
            remove { }
        }
    }

    internal static class RunQueue
    {
        static readonly Timer timer;
        static RunQueue() { timer = new Timer(Proc, null, 1, 1); }

        private static void Proc(object state)
        {
            if (Monitor.TryEnter(instances))
            {
                bool _lock = true;
                Func<bool> exec;
                try
                {
                    if (instances.Count == 0) return;
                    exec = instances[Counter];
                    Counter++;
                    Counter %= instances.Count;
                    Monitor.Exit(instances);
                    _lock = false;
                    if (exec() == false)
                    {
                        Monitor.Enter(instances);
                        _lock = true;
                        instances.Remove(exec);
                    }
                }
                finally { if (_lock) Monitor.Exit(instances); }
            }
        }

        private static int Counter;
        private static List<Func<bool>> instances = new List<Func<bool>>();

        public static void Add(Func<bool> obj)
        {
            if (instances.Contains(obj)) return;
            lock (instances)
                if (!instances.Contains(obj))
                    instances.Add(obj);
        }
    }
}
