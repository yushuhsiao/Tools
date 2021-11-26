using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ObjectDictionary = System.Collections.Generic.Dictionary<string, object>;

namespace System
{
    [DebuggerStepThrough]
    public static class _ctor
    {
        static readonly Dictionary<Thread, ObjectDictionary> all = new Dictionary<Thread, ObjectDictionary>();

        public static T GetValue<T>(T _default = default(T)) => GetValue<T>(typeof(T).FullName, _default);
        public static T GetValue<T>(string name, T _default = default(T))
        {
            if (GetValue(name, out T value))
                return value;
            return _default;
            //
            //ObjectDictionary dict;
            //object value;
            //lock (all)
            //    if (all.TryGetValue(Thread.CurrentThread, out dict) && dict.TryGetValue(name, out value))
            //        return (T)value;
            //return _default;
        }
        public static bool GetValue<T>(string name, out T value)
        {
            Thread t = Thread.CurrentThread;
            object _value;
            lock (all)
            {
                if (all.TryGetValue(t, out ObjectDictionary dict) && dict.TryGetValue(name, out _value))
                {
                    value = (T)_value;
                    return true;
                }
            }
            value = default(T);
            return false;
        }

        public static void SetValue<T>(T value) => SetValue(typeof(T).FullName, value);
        public static void SetValue<T>(string name, T value)
        {
            Thread t = Thread.CurrentThread;
            lock (all)
            {
                if (!all.ContainsKey(t))
                    all[t] = new ObjectDictionary();
                all[t][name] = value;
            }
        }

        public static TClass Create<TClass>(Func<TClass> cb, params object[] args)
        {
            try
            {
                Thread t = Thread.CurrentThread;
                for (int i = 0; i < args.Length; i++)
                {
                    if (args[i] == null) continue;
                    lock (all)
                    {
                        if (!all.TryGetValue(t, out ObjectDictionary dict))
                            all[t] = dict = new ObjectDictionary();
                        dict[args[i].GetType().FullName] = args[i];
                    }
                }
                return cb();
            }
            finally
            {
                Clear();
            }
        }
        public static TClass Create<TClass>(params object[] args) => Create(CreateInstance<TClass>);
        //public static TClass Create<TClass>(params object[] args) where TClass : new() => Create(Activator.CreateInstance<TClass>, args);
        //public static TClass Create<TClass>(params object[] args) => Create(CreateInstance<TClass>, args);
        public static TClass CreateInstance<TClass>() => (TClass)Activator.CreateInstance(typeof(TClass));

        public static void Clear()
        {
            Thread t = Thread.CurrentThread;
            lock (all)
            {
                if (all.TryGetValue(t, out ObjectDictionary dict))
                {
                    all.Remove(t);
                    dict.Clear();
                }
            }
        }
    }
}