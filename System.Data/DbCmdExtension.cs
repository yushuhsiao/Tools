using System.Collections.Generic;
using System.Diagnostics;
using _DebuggerStepThrough = System.Diagnostics.DebuggerStepThroughAttribute;

namespace System.Data
{
    [_DebuggerStepThrough]
    public static partial class DbCmdExtension
    {
        public static string sql_magic_quote(string input)
        {
            if (input == null) return null;
            return input.Replace("'", "''");
        }

        public static IDisposable BeginTransaction(this IDbConnection conn, IDbTransaction input, out IDbTransaction output)
        {
            if (input == null)
                return output = conn.BeginTransaction();
            output = input;
            return null;
        }

        public static IDisposable BeginTransaction(this IDbConnection conn, ref IDbTransaction tran)
        {
            if (tran == null)
                return tran = conn.BeginTransaction();
            return null;
        }


        public static IEnumerable<TDataReader> ForEach<TDataReader>(this TDataReader r) where TDataReader : IDataReader
        {
            do
            {
                while (r.Read())
                    yield return r;
            } while (r.NextResult());
        }

        delegate T get1_<T>(int ordinal) where T : struct;
        static Nullable<T> _get1<T>(IDataRecord r, get1_<T> getvalue, string name) where T : struct
        {
            int ordinal;
            if (r.GetOrdinal(name, out ordinal))
                return getvalue(ordinal);
            return null;
        }
        static Nullable<T> _get1<T>(IDataRecord r, get1_<T> getvalue, int ordinal) where T : struct
        {
            if ((ordinal < r.FieldCount) && !r.IsDBNull(ordinal))
                return getvalue(ordinal);
            return null;
        }

        static bool _get2<T>(IDataRecord r, get1_<T> getvalue, string name, out T result) where T : struct
        {
            int ordinal;
            if (r.GetOrdinal(name, out ordinal))
            {
                result = getvalue(ordinal);
                return true;
            }
            result = default(T);
            return false;
        }
        static bool _get2<T>(IDataRecord r, get1_<T> getvalue, int ordinal, out T result) where T : struct
        {
            if ((ordinal < r.FieldCount) && !r.IsDBNull(ordinal))
            {
                result = getvalue(ordinal);
                return true;
            }
            result = default(T);
            return false;
        }

        delegate long __get3<T>(int ordinal, long dataOffset, T[] buffer, int bufferOffset, int length) where T : struct;
        static long? _get3<T>(IDataRecord r, __get3<T> getvalue, string name, long dataIndex, T[] buffer, int bufferIndex, int length) where T : struct
        {
            int ordinal;
            if (r.GetOrdinal(name, out ordinal))
                return getvalue(ordinal, dataIndex, buffer, bufferIndex, length);
            return null;
        }
        static long? _get3<T>(IDataRecord r, __get3<T> getvalue, int ordinal, long dataIndex, T[] buffer, int bufferIndex, int length) where T : struct
        {
            if ((ordinal < r.FieldCount) && !r.IsDBNull(ordinal))
                return getvalue(ordinal, dataIndex, buffer, bufferIndex, length);
            return null;
        }

        public static string GetStringN(this IDataRecord r, string name)
        {
            int ordinal;
            if (r.GetOrdinal(name, out ordinal))
                return r.GetString(ordinal);
            return null;
        }
        public static string GetStringN(this IDataRecord r, int ordinal)
        {
            if ((ordinal < r.FieldCount) && !r.IsDBNull(ordinal))
                return r.GetString(ordinal);
            return null;
        }
        public static string GetString(this IDataRecord r, string name)
        {
            return r.GetString(r.GetOrdinal(name));
        }


        //public delegate void ForEachHandler(string s, int i);
        //public static void ForEach(this IDataRecord r, ForEachHandler d)
        //{
        //    for (int i = 0; i < r.FieldCount; i++)
        //        d(r.GetName(i), i);
        //}
        //[DebuggerStepThrough]
        //public static void Each(this IDataRecord r, Action<int> cb)
        //{
        //    for (int i = 0; i < r.FieldCount; i++) cb(i);
        //}
        //[DebuggerStepThrough]
        //public static void Each(this IDataRecord r, Action<string> cb)
        //{
        //    for (int i = 0; i < r.FieldCount; i++) cb(r.GetName(i));
        //}

        //public static IEnumerable<KeyValuePair<int, string>> ForEach(this IDataRecord r)
        //{
        //    for (int i = 0; i < r.FieldCount; i++)
        //        yield return new KeyValuePair<int, string>(i, r.GetName(i));
        //}

        public static object GetValueN(this IDataRecord r, string name)
        {
            try { return r.GetValue(r.GetOrdinal(name)); }
            catch { return null; }
        }
        public static object GetValueN(this IDataRecord r, int index)
        {
            try { return r.GetValue(index); }
            catch { return null; }
        }
        public static object GetValue(this IDataRecord r, string name)
        {
            return r.GetValue(r.GetOrdinal(name));
        }

        public static bool GetValue(this IDataRecord r, string name, out object value)
        {
            Type fieldType;
            return r.GetValue(name, out value, out fieldType);
        }
        public static bool GetValue(this IDataRecord r, string name, out object value, out Type fieldType)
        {
            for (int i = r.FieldCount - 1; i >= 0; i--)
                if (r.GetName(i) == name)
                {
                    value = r.GetValue(i);
                    fieldType = r.GetFieldType(i);
                    return true;
                }
            value = null;
            fieldType = null;
            return false;
        }

        public static bool IsDBNull(this IDataRecord r, string name)
        {
            return r.IsDBNull(r.GetOrdinal(name));
        }

        public static bool GetOrdinal(this IDataRecord r, string name, out int i)
        {
            for (i = 0; i < r.FieldCount; i++)
                if (r.GetName(i) == name)
                    if (r.IsDBNull(i))
                        break;
                    else
                        return true;
            i = -1;
            return false;
        }
        public static bool HasValue(this IDataRecord r, string name)
        {
            int i;
            return r.GetOrdinal(name, out i);
        }

        public static long? GetBytesN(this IDataRecord r, string name, long dataIndex, byte[] buffer, int bufferIndex, int length) => _get3<byte>(r, r.GetBytes, name, dataIndex, buffer, bufferIndex, length);
        public static long? GetBytesN(this IDataRecord r, int index, long dataIndex, byte[] buffer, int bufferIndex, int length) => _get3<byte>(r, r.GetBytes, index, dataIndex, buffer, bufferIndex, length);
        public static long GetBytes(this IDataRecord r, string name, long dataIndex, byte[] buffer, int bufferIndex, int length) => r.GetBytes(r.GetOrdinal(name), dataIndex, buffer, bufferIndex, length);

        public static long? GetCharsN(this IDataRecord r, string name, long dataIndex, char[] buffer, int bufferIndex, int length) => _get3<char>(r, r.GetChars, name, dataIndex, buffer, bufferIndex, length);
        public static long? GetCharsN(this IDataRecord r, int index, long dataIndex, char[] buffer, int bufferIndex, int length) => _get3<char>(r, r.GetChars, index, dataIndex, buffer, bufferIndex, length);
        public static long GetChars(this IDataRecord r, string name, long dataIndex, char[] buffer, int bufferIndex, int length) => r.GetChars(r.GetOrdinal(name), dataIndex, buffer, bufferIndex, length);

        public static string GetDataTypeName(this IDataRecord r, string name) /*                */ => r.GetDataTypeName(r.GetOrdinal(name));
        public static Type GetFieldType(this IDataRecord r, string name) /*                     */ => r.GetFieldType(r.GetOrdinal(name));
        //public static Type GetProviderSpecificFieldType(this IDataRecord r, string name) /*   */ => r.GetProviderSpecificFieldType(r.GetOrdinal(name));
        //public static object GetProviderSpecificValue(this IDataRecord r, string name) /*     */ => r.GetProviderSpecificValue(r.GetOrdinal(name));

        public static bool? GetBooleanN(this IDataRecord r, string name) /*                     */ => _get1(r, r.GetBoolean, name);
        public static bool? GetBooleanN(this IDataRecord r, int index) /*                       */ => _get1(r, r.GetBoolean, index);
        public static bool GetBoolean(this IDataRecord r, string name, out bool result) /*      */ => _get2(r, r.GetBoolean, name, out result);
        public static bool GetBoolean(this IDataRecord r, int index, out bool result) /*        */ => _get2(r, r.GetBoolean, index, out result);
        public static bool GetBoolean(this IDataRecord r, string name) /*                       */ => r.GetBoolean(r.GetOrdinal(name));

        public static byte? GetByteN(this IDataRecord r, string name) /*                        */ => _get1(r, r.GetByte, name);
        public static byte? GetByteN(this IDataRecord r, int index) /*                          */ => _get1(r, r.GetByte, index);
        public static bool GetByte(this IDataRecord r, string name, out byte result) /*         */ => _get2(r, r.GetByte, name, out result);
        public static bool GetByte(this IDataRecord r, int index, out byte result) /*           */ => _get2(r, r.GetByte, index, out result);
        public static byte GetByte(this IDataRecord r, string name) /*                          */ => r.GetByte(r.GetOrdinal(name));

        public static char? GetCharN(this IDataRecord r, string name) /*                        */ => _get1(r, r.GetChar, name);
        public static char? GetCharN(this IDataRecord r, int index) /*                          */ => _get1(r, r.GetChar, index);
        public static bool GetChar(this IDataRecord r, string name, out char result) /*         */ => _get2(r, r.GetChar, name, out result);
        public static bool GetChar(this IDataRecord r, int index, out char result) /*           */ => _get2(r, r.GetChar, index, out result);
        public static char GetChar(this IDataRecord r, string name) /*                          */ => r.GetChar(r.GetOrdinal(name));

        public static DateTime? GetDateTimeN(this IDataRecord r, string name) /*                */ => _get1(r, r.GetDateTime, name);
        public static DateTime? GetDateTimeN(this IDataRecord r, int index) /*                  */ => _get1(r, r.GetDateTime, index);
        public static bool GetDateTime(this IDataRecord r, string name, out DateTime result) /* */ => _get2(r, r.GetDateTime, name, out result);
        public static bool GetDateTime(this IDataRecord r, int index, out DateTime result) /*   */ => _get2(r, r.GetDateTime, index, out result);
        public static DateTime GetDateTime(this IDataRecord r, string name) /*                  */ => r.GetDateTime(r.GetOrdinal(name));

        public static decimal? GetDecimalN(this IDataRecord r, string name) /*                  */ => _get1(r, r.GetDecimal, name);
        public static decimal? GetDecimalN(this IDataRecord r, int index) /*                    */ => _get1(r, r.GetDecimal, index);
        public static bool GetDecimal(this IDataRecord r, string name, out decimal result) /*   */ => _get2(r, r.GetDecimal, name, out result);
        public static bool GetDecimal(this IDataRecord r, int index, out decimal result) /*     */ => _get2(r, r.GetDecimal, index, out result);
        public static decimal GetDecimal(this IDataRecord r, string name) /*                    */ => r.GetDecimal(r.GetOrdinal(name));

        public static double? GetDoubleN(this IDataRecord r, string name) /*                    */ => _get1(r, r.GetDouble, name);
        public static double? GetDoubleN(this IDataRecord r, int index) /*                      */ => _get1(r, r.GetDouble, index);
        public static bool GetDouble(this IDataRecord r, string name, out double result) /*     */ => _get2(r, r.GetDouble, name, out result);
        public static bool GetDouble(this IDataRecord r, int index, out double result) /*       */ => _get2(r, r.GetDouble, index, out result);
        public static double GetDouble(this IDataRecord r, string name) /*                      */ => r.GetDouble(r.GetOrdinal(name));

        public static float? GetFloatN(this IDataRecord r, string name) /*                      */ => _get1(r, r.GetFloat, name);
        public static float? GetFloatN(this IDataRecord r, int index) /*                        */ => _get1(r, r.GetFloat, index);
        public static bool GetFloat(this IDataRecord r, string name, out float result) /*       */ => _get2(r, r.GetFloat, name, out result);
        public static bool GetFloat(this IDataRecord r, int index, out float result) /*         */ => _get2(r, r.GetFloat, index, out result);
        public static float GetFloat(this IDataRecord r, string name) /*                        */ => r.GetFloat(r.GetOrdinal(name));

        public static Guid? GetGuidN(this IDataRecord r, string name) /*                        */ => _get1(r, r.GetGuid, name);
        public static Guid? GetGuidN(this IDataRecord r, int index) /*                          */ => _get1(r, r.GetGuid, index);
        public static bool GetGuid(this IDataRecord r, string name, out Guid result) /*         */ => _get2(r, r.GetGuid, name, out result);
        public static bool GetGuid(this IDataRecord r, int index, out Guid result) /*           */ => _get2(r, r.GetGuid, index, out result);
        public static Guid GetGuid(this IDataRecord r, string name) /*                          */ => r.GetGuid(r.GetOrdinal(name));

        public static short? GetInt16N(this IDataRecord r, string name) /*                      */ => _get1(r, r.GetInt16, name);
        public static short? GetInt16N(this IDataRecord r, int index) /*                        */ => _get1(r, r.GetInt16, index);
        public static bool GetInt16(this IDataRecord r, string name, out short result) /*       */ => _get2(r, r.GetInt16, name, out result);
        public static bool GetInt16(this IDataRecord r, int index, out short result) /*         */ => _get2(r, r.GetInt16, index, out result);
        public static short GetInt16(this IDataRecord r, string name) /*                        */ => r.GetInt16(r.GetOrdinal(name));

        public static int? GetInt32N(this IDataRecord r, string name) /*                        */ => _get1(r, r.GetInt32, name);
        public static int? GetInt32N(this IDataRecord r, int index) /*                          */ => _get1(r, r.GetInt32, index);
        public static bool GetInt32(this IDataRecord r, string name, out int result) /*         */ => _get2(r, r.GetInt32, name, out result);
        public static bool GetInt32(this IDataRecord r, int index, out int result) /*           */ => _get2(r, r.GetInt32, index, out result);
        public static int GetInt32(this IDataRecord r, string name) /*                          */ => r.GetInt32(r.GetOrdinal(name));

        public static long? GetInt64N(this IDataRecord r, string name) /*                       */ => _get1(r, r.GetInt64, name);
        public static long? GetInt64N(this IDataRecord r, int index) /*                         */ => _get1(r, r.GetInt64, index);
        public static bool GetInt64(this IDataRecord r, string name, out long result) /*        */ => _get2(r, r.GetInt64, name, out result);
        public static bool GetInt64(this IDataRecord r, int index, out long result) /*          */ => _get2(r, r.GetInt64, index, out result);
        public static long GetInt64(this IDataRecord r, string name) /*                         */ => r.GetInt64(r.GetOrdinal(name));
    }
}