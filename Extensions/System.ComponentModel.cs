namespace System.ComponentModel
{
    public static class DefaultValueAttributeExtension
    {
        public static T GetValue<T>(this DefaultValueAttribute a)
        {
            if (a != null)
            {
                if (a.Value != null)
                {
                    if (a.Value.GetType() == typeof(T))
                    {
                        try { return (T)a.Value; }
                        catch { }
                    }
                }
                TypeConverter c = TypeDescriptor.GetConverter(typeof(T));
                if (c.CanConvertTo(typeof(T)))
                {
                    try { return (T)c.ConvertTo(a.Value, typeof(T)); }
                    catch { }
                }
            }
            return default(T);
        }
    }
}
