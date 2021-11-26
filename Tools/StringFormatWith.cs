using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
//using SqlCmd = System.Data.SqlClient.SqlCmd;

namespace System
{
    //[DebuggerStepThrough]
    public static class StringFormatWith
    {
        public interface StringValue { }
        public interface RawSqlString { }
        public const string sql_varchar = "varchar";
        public const string sql_nvarchar = "nvarchar";
        public const string sql_array = "[array]";
        public const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        public const string SqlDateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";
        public const string DateTimeFormatEx = DateTimeFormat + ".ffffff";
        public static string FormatAs(this DateTime dateTime, string format = DateTimeFormat, int ms_digit = 0)
        {
            if (ms_digit <= 0)
                return dateTime.ToString(format);
            if (ms_digit > 6)
                ms_digit = 6;
            return dateTime.ToString(format + "." + new String('f', ms_digit));
        }
        public static string DateTimeText(this DateTime dateTime) => dateTime.ToString(DateTimeFormat);

        private class _raw_string : RawSqlString
        {
            public string value;
            public override string ToString() => value;
        }
        //public static readonly RawSqlString raw_getdate = new _raw_string { value = "getdate()" };
        internal static readonly RawSqlString raw_null = new _raw_string { value = "null" };
        //public static readonly RawSqlString raw_newid = new _raw_string { value = "newid()" };


        [DebuggerStepThrough]
        public static string sql_magic_quote(string input)
        {
            if (input == null) return null;
            return input.Replace("'", "''");
        }

        //public static string format(this string format, object arg0)
        //{
        //    return string.Format(format, arg0);
        //}
        //public static string format(this string format, object arg0, object arg1)
        //{
        //    return string.Format(format, arg0, arg1);
        //}
        //public static string format(this string format, object arg0, object arg1, object arg2)
        //{
        //    return string.Format(format, arg0, arg1, arg2);
        //}

        //static bool TryGetValue(object obj, Type objType, string name, out object value)
        //{
        //    try
        //    {
        //        if (name == "")
        //        {
        //            value = obj;
        //            return value != null;
        //        }
        //        string l, r;
        //        name.Split('.', out l, out r);
        //        IDictionary<string, object> dict = obj as IDictionary<string, object>;
        //        if ((dict != null) && (dict.TryGetValue(l, out value)))
        //        {
        //            //if (r == "")
        //            //    return true;
        //            //if (value == null)
        //            //    return false;
        //            return TryGetValue(value, value.GetType(), r, out value);
        //        }

        //        TypeContract t = TypeContract.GetContract(objType);
        //        FieldInfo f;
        //        if (t.GetMember(l, out f))
        //        {
        //            value = f.GetValue(f.IsStatic ? null : obj);
        //            return TryGetValue(value, f.FieldType, r, out value);
        //        }
        //        PropertyInfo p;
        //        if (t.GetMember(l, out p))
        //        {
        //            value = p.GetValue(p.IsStatic() ? null : obj, null);
        //            return TryGetValue(value, p.PropertyType, r, out value);
        //        }
        //    }
        //    catch { }
        //    value = null;
        //    return false;
        //}

        public delegate bool TryGetValueHander(object obj, string name, out object value);
        private static bool TryGetValue(object obj, string name, out object value)
        {
            value = default(object);
            return false;
        }

        public static StringBuilder FormatWith(this string format, StringBuilder str, object obj, bool sql = false, TryGetValueHander getValue = null)
        {
            if (string.IsNullOrEmpty(format)) return str;
            if (obj == null) return str;
            Type objType = obj.GetType();
            getValue = getValue ?? TryGetValue;
            str = str ?? new StringBuilder();
            char c;
            for (int pos = 0, len = format.Length; -1 < len;)
            {
                if (pos >= len) break;
                c = format[pos++];
                if (c == '{')
                {
                    if (pos < len && format[pos] == '{')
                        pos++;
                    else
                    {
                        for (int bracket = pos, colon = -1; pos < len;)
                        {
                            if (pos >= len) break;
                            c = format[pos++];
                            if (c == '}')
                            {
                                string name, fmt;
                                if (colon == -1)
                                {
                                    name = format.Substring(bracket, pos - bracket - 1);
                                    fmt = null;
                                }
                                else
                                {
                                    name = format.Substring(bracket, colon - bracket - 1);
                                    fmt = format.Substring(colon, pos - colon - 1);
                                }
                                object value;
                                if (sql && name == "")
                                {
                                    if (fmt == "TableName")
                                    {
                                        string tableName = System.Data.TableName.GetInstance(obj).TableName;
                                        if (!string.IsNullOrEmpty(tableName))
                                        {
                                            //str.Append('[');
                                            str.Append(tableName);
                                            //str.Append(']');
                                            fmt = null;
                                        }
                                    }
                                    if (fmt != null)
                                    {
                                        str.Append('@');
                                        str.Append(fmt);
                                    }
                                }
                                else if (getValue(obj, name, out value) || objType.TryGetValue(obj, name, out value))
                                {
                                    if (sql)
                                        str.AppendAsSqlString(fmt, value);
                                    else if (fmt == null)
                                        str.Append(value);
                                    else
                                        str.AppendFormat("{0:" + fmt + "}", value);
                                }
                                else
                                {
                                    if (name.ToInt32().HasValue)
                                    {
                                        str.Append('{');
                                        str.Append(name);
                                        str.Append('}');
                                    }
                                    else if (sql)
                                    {
                                        str.Append('@');
                                        str.Append(name);
                                    }
                                }
                                break;
                            }
                            else if (c == ':')
                            {
                                if (colon == -1)
                                    colon = pos;
                            }
                        }
                        continue;
                    }
                }
                else if (c == '}')
                {
                    if (pos < len && format[pos] == '}')
                        pos++;
                }
                str.Append(c);
            }
            return str;
        }

        //const string _nvarchar = "{0:" + SqlCmd.nvarchar + "}";

        //[DebuggerStepThrough]
        //public static string formatWith(this string format, object obj, bool sql = false) { return StringExtensions.FormatWith(format, obj, sql); }
        public static string FormatWith(this string format, object obj, bool sql = false, TryGetValueHander getValue = null)
            => format.FormatWith(default(StringBuilder), obj, sql, getValue)?.ToString();


        //{
        //    if (string.IsNullOrEmpty(format)) return format;
        //    if (obj == null) return format;
        //    Type objType = obj.GetType();
        //    return format.FormatWith(new StringBuilder(), obj, sql).ToString();
        //}

        private static bool sql_string(ref string fmt, ref object value, out bool nvarchar)
        {
            string _fmt = fmt ?? "";
            bool varchar = _fmt.StartsWith(sql_varchar);
            nvarchar = _fmt.StartsWith(sql_nvarchar);
            string _value;
            if (value is string)
                _value = value as string;
            else if (value is StringValue)
                _value = ((StringValue)value).ToString();
            else if (varchar || nvarchar)
                _value = value.ToString();
            else
                return false;

            int len = -1;

            if (varchar || nvarchar)
            {
                fmt = null;
                _fmt.Substring('(', ')', false).ToInt32(out len);
            }
            else if (_fmt.ToInt32(out len))
            {
                fmt = null;
            }
            else
            {
                len = -1;
            }
            if (len > 0 && _value.Length > len)
                value = StringFormatWith.sql_magic_quote(_value.Substring(0, len));
            else
                value = StringFormatWith.sql_magic_quote(_value);
            return true;
        }

        private static StringBuilder AppendAsSqlString(this StringBuilder str, string fmt, object value)
        {
            bool quote = false;
            if (value == null)
                value = "null";
            else if ((value is IList) && (fmt == StringFormatWith.sql_array))
            {
                IList list = (IList)value;
                if (list.Count > 0)
                {
                    str.Append('(');
                    for (int i = 0; i < list.Count; i++)
                    {
                        if (i > 0) str.Append(',');
                        str.AppendAsSqlString(null, list[i]);
                    }
                    str.Append(')');
                }
                return str;
            }
            else if (value is RawSqlString)
            {
                quote = false;
                value = value.ToString();
            }
            else if (sql_string(ref fmt, ref value, out bool nvarchar))
            {
                quote = true;
                if (nvarchar)
                    str.Append('N');
            }
            else if (value is Guid | value is DateTime)
                quote = true;
            else if (value is bool)
            {
                quote = false;
                value = ((bool)value).ToInt32();
            }
            else
            {
                Type t = value.GetType();
                if (t.IsEnum)
                    value = Convert.ChangeType(value, Enum.GetUnderlyingType(t));
            }
            if (quote) str.Append('\'');
            if (fmt == null)
                str.Append(value);
            else
                str.AppendFormat("{0:" + fmt + "}", value);
            if (quote) str.Append('\'');
            return str;
        }

        //public static string ToSqlString(this IList list)
        //{
        //    StringBuilder s = new StringBuilder();
        //    StringExtensions.ToSqlString(list, s);
        //    return s.ToString();
        //}
        //public static StringBuilder ToSqlString(this IList list, StringBuilder str)
        //{
        //    if (list == null) return str;
        //    if (list.Count == 0) return str;
        //    return str.AppendAsSqlString(SqlCmd.array, list);
        //}
        //public static StringBuilder ToString(this IList list, Func<StringBuilder> str)
        //{
        //    if (list == null) return null;
        //    if (list.Count == 0) return null;
        //    return list.ToSqlString(str?.Invoke());
        //}

        //static string SqlExport(this string str, object obj)
        //{
        //    return StringExtensions.StringExport(str, null, obj, true);
        //}
        //static string SqlExport(this string str, string id, object obj)
        //{
        //    return StringExtensions.StringExport(str, id, obj, true);
        //}
        //static void SqlExport(this StringBuilder dst, string str, string id, object obj)
        //{
        //    StringExport(str, dst, id, obj, false);
        //}

        //static string StringExport(this string str, object obj)
        //{
        //    return StringExtensions.StringExport(str, null, obj, false);
        //}
        //static string StringExport(this string str, string id, object obj)
        //{
        //    return StringExtensions.StringExport(str, id, obj, false);
        //}

        //static string StringExport(string str, string id, object obj, bool sql)
        //{
        //    StringBuilder dst = new StringBuilder();
        //    StringExport(str, dst, id, obj, sql);
        //    return dst.ToString();
        //}
        //static void StringExport(string str, StringBuilder dst, string id, object obj, bool sql)
        //{
        //    Dictionary<string, object> obj_dict = obj as Dictionary<string, object>;
        //    StringExportContract contract = StringExportContract.GetContract(obj, id);
        //    int? n1 = null, n2 = null;
        //    for (int i = 0; i < str.Length; i++)
        //    {
        //        char c = str[i];
        //        if (n1.HasValue)
        //        {
        //            if ((c == ':') && !n2.HasValue)
        //                n2 = i;
        //            else if (c == '}')
        //            {
        //                string field;
        //                string fmt1;
        //                string fmt2;
        //                if (n2.HasValue)
        //                {
        //                    field = str.Substring(n1.Value, n2.Value - n1.Value);
        //                    fmt1 = str.Substring(n2.Value + 1, i - n2.Value - 1);
        //                    fmt2 = string.Format("{{0:{0}}}", fmt1);
        //                }
        //                else
        //                {
        //                    field = str.Substring(n1.Value, i - n1.Value);
        //                    fmt1 = null;
        //                    fmt2 = "{0}";
        //                }
        //                int nnn;
        //                bool hasValue;
        //                object value;
        //                if (obj_dict == null)
        //                {
        //                    hasValue = false;
        //                    value = null;
        //                    if (contract.ContainsKey(field))
        //                    {
        //                        PropertyInfo p = contract[field].p;
        //                        FieldInfo f = contract[field].f;
        //                        if ((p != null) && (p.CanRead))
        //                        {
        //                            value = p.GetValue(p.IsStatic() ? null : obj, null);
        //                            hasValue = true;
        //                        }
        //                        else if (f != null)
        //                        {
        //                            value = f.GetValue(f.IsStatic ? null : obj);
        //                            hasValue = true;
        //                        }
        //                    }
        //                }
        //                else if (hasValue = obj_dict.ContainsKey(field))
        //                    value = obj_dict[field];
        //                else
        //                    value = null;

        //                if (hasValue)
        //                {
        //                    if (sql)
        //                    {
        //                        bool quote = (value is string) || (value is Guid) || (value is DateTime);
        //                        if (value == null)
        //                            value = "null";
        //                        else
        //                        {
        //                            Type t = value.GetType();
        //                            if (t.IsEnum)
        //                                value = Convert.ChangeType(value, Enum.GetUnderlyingType(t));
        //                        }
        //                        if (quote) dst.Append('\'');
        //                        dst.AppendFormat(fmt2, value);
        //                        if (quote) dst.Append('\'');
        //                    }
        //                    else
        //                        dst.AppendFormat(fmt2, value);
        //                }
        //                else if (sql && !int.TryParse(field, out nnn))
        //                {
        //                    dst.Append('@');
        //                    dst.Append(field);
        //                }
        //                else
        //                {
        //                    dst.Append('{');
        //                    dst.AppendFormat(fmt2, field);
        //                    dst.Append('}');
        //                }
        //                n1 = n2 = null;
        //            }
        //        }
        //        else if (c == '{')
        //        {
        //            n1 = i + 1;
        //        }
        //        else
        //            dst.Append(c);
        //    }
        //}
    }
}