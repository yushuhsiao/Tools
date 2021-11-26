using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace System.Data
{
    public interface ITableName
    {
        string TableName { get; }
        string Database { get; }
        string SortKey { get; }
    }

    [DebuggerStepThrough]
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = true)]
    public sealed class TableNameAttribute : Attribute, ITableName
    {
        public string TableName { get; private set; }
        public Type ClassType { get; private set; }
        public string Database { get; set; }
        public string SortKey { get; set; }

        public TableNameAttribute(string tableName)
        {
            this.TableName = tableName;
            this.ClassType = _ctor.GetValue("ClassType", default(Type));
        }

        public TableNameAttribute(Type fromType) : this("")
        {
            if (this.ClassType != fromType)
            {
                ITableName attr = System.Data.TableName.GetInstance(fromType);
                this.TableName = attr.TableName;
                this.Database = attr.Database;
            }
        }

    }

    [DebuggerStepThrough]
    public static class TableName
    {
        public interface IGetInstance
        {
            ITableName this[Type classType] { get; }
            ITableName this[object obj] { get; }
        }
        private class _GetInstance : IGetInstance
        {
            public ITableName this[Type classType] => TableName.GetInstance(classType);
            public ITableName this[object obj] => TableName.GetInstance(obj);
        }
        public interface IGetValue
        {
            string this[Type classType] { get; }
            string this[object obj] { get; }
        }
        private class _GetValue : IGetValue
        {
            public string this[Type classType] => TableName.GetInstance(classType).TableName;
            public string this[object obj] => TableName.GetInstance(obj).TableName;
        }
        public static readonly IGetInstance _ = new _GetInstance();
        public static readonly IGetValue Value = new _GetValue();


        public static ITableName GetInstance(Type classType)
        {
            if (classType == null) return null;
            Type t = typeof(TableName<>).MakeGenericType(classType);
            PropertyInfo p = t.GetProperty(nameof(TableName<object>._), BindingFlags.Static | BindingFlags.Public);
            return p?.GetValue(null) as ITableName;
        }
        public static ITableName GetInstance<T>() => TableName<T>._;
        public static ITableName GetInstance(object obj)
        {
            if (obj is ITableName)
                return (ITableName)obj;
            return GetInstance(obj?.GetType());
        }
    }

    [DebuggerStepThrough]
    public static class TableName<T>
    {
        private static ITableName _instance;
        static TableName()
        {
            _instance = typeof(T).GetCustomAttribute<TableNameAttribute>(true) ?? new TableNameAttribute("");
        }
        public static ITableName _
        {
            get
            {
                ITableName obj = Interlocked.CompareExchange(ref _instance, null, null);
                if (obj != null) return obj;
                try
                {
                    _ctor.SetValue("ClassType", typeof(T));
                    obj = typeof(T).GetCustomAttribute<TableNameAttribute>(true) ?? new TableNameAttribute("");
                    Interlocked.Exchange(ref _instance, obj);
                    return obj;
                }
                finally
                {
                    _ctor.Clear();
                }
            }
            //get { return TableNameAttribute.GetInstance<T>(); }
        }

        public static string Value => _.TableName;
    }
}
