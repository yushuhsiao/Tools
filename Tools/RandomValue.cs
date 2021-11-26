using System.Text;

using _DebuggerStepThrough = System.Diagnostics.FakeDebuggerStepThroughAttribute;
namespace System.Security.Cryptography
{
    [_DebuggerStepThrough]
    public unsafe static class RandomValue
    {
        public static readonly RNGCryptoServiceProvider RNG = new RNGCryptoServiceProvider();

        public static byte[] GetBytes(this RandomNumberGenerator rand, int size)
        {
            byte[] n = new byte[size];
            rand.GetBytes(n);
            return n;
        }
        public static byte[] GetBytes(int size)
        {
            return RNG.GetBytes(size);
        }

        public static Int16 GetInt16(this RandomNumberGenerator rand, Int16 max = Int16.MaxValue)
        {
            fixed (void* p = rand.GetBytes(sizeof(Int16)))
            {
                Int16 v = *(Int16*)p;
                if (v < 0) v *= -1;
                v %= max;
                return v;
            }
        }
        public static Int16 GetInt16(Int16 max = Int16.MaxValue)
        {
            return RandomValue.RNG.GetInt16(max);
        }

        public static Int32 GetInt32(this RandomNumberGenerator rng, Int32 max = Int32.MaxValue)
        {
            fixed (void* p = rng.GetBytes(sizeof(Int32)))
            {
                Int32 v = *(Int32*)p;
                if (v < 0) v *= -1;
                v %= max;
                return v;
            }
        }
        public static Int32 GetInt32(Int32 max = Int32.MaxValue)
        {
            return RandomValue.RNG.GetInt32(max);
        }


        public static Int64 GetInt64(this RandomNumberGenerator rng, Int64 max = Int64.MaxValue)
        {
            fixed (void* p = rng.GetBytes(sizeof(Int64)))
            {
                Int64 v = *(Int64*)p;
                if (v < 0) v *= -1;
                v %= max;
                return v;
            }
        }
        public static Int64 GetInt64(Int64 max = Int64.MaxValue)
        {
            return RandomValue.RNG.GetInt64(max);
        }

        public const string Number = "0123456789";
        public const string LowerLetter = "abcdefghijklmnopqrstuvwxyz";
        public const string LowerNumber = LowerLetter + Number;
        public const string UpperLetter = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public const string UpperNumber = UpperLetter + Number;

        public static string GetRandomString(int minLength, int maxLength, string pattern = RandomValue.LowerNumber)
        {
            int length;
            if (minLength == maxLength)
                length = minLength;
            else
            {
                int range = Math.Abs(maxLength - minLength);
                length = RandomValue.GetInt32() % range;
                length += Math.Min(minLength, maxLength);
            }
            return RandomValue.GetRandomString(length, pattern);
        }
        public static string GetRandomString(int length, string pattern = RandomValue.LowerNumber)
        {
            if (pattern != null)
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < length; i++)
                    sb.Append(pattern[RandomValue.GetInt32(pattern.Length)]);
                return sb.ToString();
            }
            return pattern;
        }
    }
}
