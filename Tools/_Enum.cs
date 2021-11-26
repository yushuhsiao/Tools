using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace System
{
    public static class _Enum<T>
    {
        public static IEnumerable<T> GetValuesWithOut(params T[] excludes)
        {
            var values = Enum.GetValues(typeof(T));
            for (int i = 0; i < values.Length; i++)
            {
                var n = (T)values.GetValue(i);
                if (excludes.Contains(n))
                    continue;
                else
                    yield return n;
            }
        }

        public static T Parse(string value) => (T)Enum.Parse(typeof(T), value);
    }
}
