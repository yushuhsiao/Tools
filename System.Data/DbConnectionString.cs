using System.ComponentModel;
using System.Globalization;

namespace System.Data
{
    [TypeConverter(typeof(DbConnectionString._TypeConverter))]
    public struct DbConnectionString
    {
        //public int Index { get; }
        public string Value { get; }
        public DbConnectionString(string value, int index = 0)
        {
            if (value == null)
                this.Value = value;
            else
                this.Value = value.Trim();
            //this.Index = 0;
        }
        public override string ToString() => this.Value;

        public bool IsEmpty => string.IsNullOrEmpty(this.Value);

        public static implicit operator string(DbConnectionString value) => value.Value;
        public static implicit operator DbConnectionString(string value) => new DbConnectionString(value);

        public static bool operator ==(DbConnectionString src, DbConnectionString obj) => src.Equals(obj);
        public static bool operator !=(DbConnectionString src, DbConnectionString obj) => !src.Equals(obj);

        public override int GetHashCode() => base.GetHashCode();

        public override bool Equals(object obj)
        {
            if (obj is DbConnectionString)
                return this.Value == ((DbConnectionString)obj).Value;
            return false;
        }

        class _TypeConverter : TypeConverter
        {
            public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
            {
                if (sourceType == typeof(string))
                    return true;
                if (sourceType == typeof(object))
                    return true;
                return base.CanConvertFrom(context, sourceType);
            }

            public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
            {
                if (value is string)
                    return new DbConnectionString((string)value);
                return base.ConvertFrom(context, culture, value);
            }

            public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
            {
                if (destinationType == typeof(DbConnectionString))
                    return true;
                return base.CanConvertTo(context, destinationType);
            }

            public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
            {
                if (value is string && destinationType == typeof(DbConnectionString))
                    return new DbConnectionString((string)value);
                return base.ConvertTo(context, culture, value, destinationType);
            }
        }
    }

    //public static partial class _SqlConnectionStringExtension
    //{
    //    [DebuggerStepThrough]
    //    public static T ExecSql<T>(this DbConnectionString cn, object sync, ref SqlCmd location, Func<SqlCmd, T> cb, bool throwException = true)
    //    {
    //        bool _locked = false;
    //        for (int i = 0; ; i++)
    //        {
    //            try
    //            {
    //                if (_Monitor.TryEnterN(sync, 10, ref _locked))
    //                {
    //                    if ((location != null) && (location.ConnectionString != cn))
    //                        using (IDisposable x = location)
    //                            location = null;
    //                    location = location ?? new SqlCmd(cn);
    //                    //_Monitor.Exit(sync, ref _locked);
    //                    return cb(location);
    //                }
    //            }
    //            catch (Exception ex)
    //            {
    //                log.error(ex);
    //                while (!_Monitor.TryEnterN(sync, 10, ref _locked))
    //                    continue;
    //                //if (_locked == false) _Monitor.Enter(sync, ref _locked);
    //                using (IDisposable x = location)
    //                    location = null;
    //                if (throwException)
    //                    throw;
    //                return default(T);
    //            }
    //            finally
    //            {
    //                _Monitor.ExitN(sync, ref _locked);
    //            }
    //        }
    //    }
    //}
}