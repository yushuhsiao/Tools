using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using _DebuggerStepThrough = System.Diagnostics.DebuggerStepThroughAttribute;

namespace System
{
    [_DebuggerStepThrough]
    public static class _TypeExtensions
    {
        public static bool IsSubclassOf(this Type type, Type c, bool include_self = false)
        {
            if (type == null) return false;
            bool n = type.IsSubclassOf(c);
            if (include_self) n |= type == c;
            return n;
        }

        public static bool IsSubclassOf<T>(this Type type, bool include_self = false)
        {
            return IsSubclassOf(type, typeof(T), include_self);
        }

        public static bool IsEquals<T>(this Type type)
        {
            return type == typeof(T);
        }

        public static bool Is<T>(this Type t)
        {
            return t == typeof(T);
        }

        public static bool Is<T>(this Type t, bool include_nullable = false) where T : struct
        {
            if (t == typeof(T))
                return true;
            else if (t.IsNullable(out var tt))
                return tt == typeof(T);
            else
                return false;
        }

        public static bool HasInterface<T>(this Type t)
        {
            if (t != null)
            {
                var tmp = t.GetInterfaces();
                for (int n = 0; n < tmp.Length; n++)
                    if (tmp[n] == typeof(T))
                        return true;
            }
            return false;
        }

        public static bool HasInterface(this Type t, Type i)
        {
            if (t != null)
            {
                if (i.IsGenericTypeDefinition)
                {
                    var tmp = t.GetInterfaces();
                    for (int n = 0; n < tmp.Length; n++)
                        if (tmp[n].IsGenericType && tmp[n].GetGenericTypeDefinition() == i)
                            return true;
                }
                else
                {
                    var tmp = t.GetInterfaces();
                    for (int n = 0; n < tmp.Length; n++)
                        if (tmp[n] == i)
                            return true;
                }
            }
            return false;
        }

        public static bool IsNullable(this Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static bool IsNullable(this Type t, out Type type)
        {
            if (t.IsNullable())
            {
                type = t.GetGenericArguments()[0];
                return true;
            }
            type = null;
            return false;
        }

        public static bool IsStatic(this PropertyInfo p)
        {
            if (p != null)
            {
                MethodInfo _get = p.GetGetMethod();
                if (_get != null) return _get.IsStatic;
                MethodInfo _set = p.GetSetMethod();
                if (_set != null) return _set.IsStatic;
            }
            return false;
        }

        //public static T Cast<T>(this object obj) => (T)obj;

        //public static T TryCast<T>(this object obj)
        //{
        //    if (obj is T)
        //        return (T)obj;
        //    return default(T);
        //}

        //public static bool TryCast<T>(this object obj, out T result)
        //{
        //    bool r = obj is T;
        //    if (r)
        //        result = (T)obj;
        //    else
        //        result = default(T);
        //    return r;
        //}
    }
}
