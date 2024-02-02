using System.Text;

namespace System.Runtime.InteropServices
{
    public static class StringUtil
    {
        public unsafe static void StringToPtr(string src, byte* dst, int len, Encoding encoding = null)
        {
            if (string.IsNullOrEmpty(src)) return;
            encoding = encoding ?? Encoding.ASCII;
            byte[] data = encoding.GetBytes(src);
            Marshal.Copy(data, 0, (IntPtr)dst, Math.Min(data.Length, len));
            //char[] c1 = src.ToCharArray();
            //for (int i = 0; i < len && i < c1.Length; i++)
            //    *dst++ = (byte)c1[i];
        }
        public unsafe static void StringToPtr(string src, char* dst, int len, Encoding encoding = null)
        {
            if (string.IsNullOrEmpty(src)) return;
            encoding = encoding ?? Encoding.Unicode;
            byte[] data = encoding.GetBytes(src);
            Marshal.Copy(data, 0, (IntPtr)dst, Math.Min(data.Length, len * 2));
            //char[] c1 = src.ToCharArray();
            //for (int i = 0; i < len && i < c1.Length; i++)
            //    *dst++ = (byte)c1[i];
        }

        public unsafe static string PtrToString(byte* src, int len, Func<string, string> filter = null)
        {
            StringBuilder s = new StringBuilder();
            var p = src;
            for (int i = 0; i < len; i++, p++)
            {
                if (*p == 0)
                {
                    if (s.Length == 0)
                        continue;
                    else
                        break;
                }
                s.Append((char)*p);
            }
            return PtrToString(s, filter);
        }
        public unsafe static string PtrToString(char* src, int len, Func<string, string> filter = null)
        {
            StringBuilder s = new StringBuilder();
            var p = src;
            for (int i = 0; i < len; i++, p++)
            {
                if (*p == 0)
                {
                    if (s.Length == 0)
                        continue;
                    else
                        break;
                }
                s.Append(*p);
            }
            return PtrToString(s, filter);
        }
        private unsafe static string PtrToString(StringBuilder s, Func<string, string> filter)
        {
            if (filter == null)
                return s.ToString();
            else
                return filter(s.ToString());
        }

        public static string TrimFirstComma(string s)
        {
            if (s.StartsWith(","))
                return s.Substring(1);
            return s;
        }
    }
}