using Dapper;
using System;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace System.Data
{
    [TypeConverter(typeof(_TypeConverter))]
    [JsonConverter(typeof(_JsonConverter))]
    public struct SqlTimeStamp
    {
        static SqlTimeStamp()
        {
            SqlMapper.AddTypeHandler(typeof(SqlTimeStamp), new _TypeHandler());
            TypeDescriptor.AddAttributes(typeof(byte[]), new TypeConverterAttribute(typeof(_TypeConverter)));
        }

        public byte[] data;
        public static implicit operator long(SqlTimeStamp t)
        {
            if (t.data == null) return 0;
            long result = 0;
            for (int i = 0; i < t.data.Length; i++)
            {
                result <<= 8;
                result |= t.data[i];
            }
            return result;
        }
        public static implicit operator SqlTimeStamp(long t)
        {
            byte[] data = new byte[8];
            for (int i = data.Length - 1; i <= 0; i--)
            {
                long tt = t;
                tt &= 0xff;
                data[i] = (byte)tt;
                t >>= 8;
            }
            return (SqlTimeStamp)data;
        }
        public static implicit operator byte[](SqlTimeStamp t)
        {
            return t.data;
        }
        public static implicit operator SqlTimeStamp(byte[] data)
        {
            return new SqlTimeStamp() { data = data };
        }
        //public static bool Create(object data, out SqlTimeStamp value)
        //{
        //    if (data is byte[])
        //        value = (SqlTimeStamp)(byte[])data;
        //    else if (data is long)
        //        value = (SqlTimeStamp)(long)data;
        //    else
        //    {
        //        value = default(SqlTimeStamp);
        //        return false;
        //    }
        //    return true;
        //}

        class _TypeHandler : SqlMapper.ITypeHandler
        {
            object SqlMapper.ITypeHandler.Parse(Type destinationType, object value)
            {
                if (destinationType == typeof(SqlTimeStamp) && value is byte[] _value)
                    return (SqlTimeStamp)_value;
                return null;
            }

            void SqlMapper.ITypeHandler.SetValue(IDbDataParameter parameter, object value)
            {
                throw new NotImplementedException();
            }
        }

        class _TypeConverter : TypeConverter
        {
            public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
            {
                return sourceType == typeof(byte[]);
            }
            public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
            {
                return base.CanConvertTo(context, destinationType);
            }
            public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
            {
                byte[] data = value as byte[];
                if (data != null)
                    return new SqlTimeStamp() { data = data };
                return base.ConvertFrom(context, culture, value);
            }
            public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
            {
                return base.ConvertTo(context, culture, value, destinationType);
            }
        }

        class _JsonConverter : JsonConverter<SqlTimeStamp>
        {
            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(SqlTimeStamp);
            }

            public override SqlTimeStamp Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return reader.GetInt64();
                //throw new NotImplementedException();
            }

            //public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            //{
            //    return (SqlTimeStamp)serializer.Deserialize<long>(reader);
            //}

            public override void Write(Utf8JsonWriter writer, SqlTimeStamp value, JsonSerializerOptions options)
            {
                writer.WriteNumberValue(value);
            }

            //public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            //{
            //    if (value is SqlTimeStamp)
            //    {
            //        SqlTimeStamp _value = (SqlTimeStamp)value;
            //        serializer.Serialize(writer, (long)_value);
            //    }
            //    else
            //        serializer.Serialize(writer, value);
            //}
        }
    }
}
