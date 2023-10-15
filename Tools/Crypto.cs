using System.IO;
using System.Text;
using _DebuggerStepThrough = System.Diagnostics.FakeDebuggerStepThroughAttribute;

namespace System.Security.Cryptography
{
    [_DebuggerStepThrough]
    public static partial class Crypto
    {
        //public static string ToBase64String(byte[] input) => Convert.ToBase64String(input);
        //public static byte[] FromBase64String(string input) => Convert.FromBase64String(input);

        public static string MD5(this string input)
        {
            return MD5(input, null);
        }
        public static string MD5(this string input, Encoding encoding)
        {
            return Convert.ToBase64String(MD5((encoding ?? Encoding.UTF8).GetBytes(input)));
        }
        public static byte[] MD5(this byte[] input)
        {
            using (var md5 = Cryptography.MD5.Create()) return md5.ComputeHash(input);
            //using (MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider()) return md5.ComputeHash(input);
        }

        //public static string MD5Hex(this string input)
        //{
        //    return MD5Hex(input, null);
        //}
        //public static string MD5Hex(this string input, Encoding encoding)
        //{
        //    return MD5((encoding ?? Encoding.UTF8).GetBytes(input)).ToHex();
        //}

        //public static string SHA1Hex(this string input)
        //{
        //    return SHA1Hex(input, null);
        //}
        //public static string SHA1Hex(this string input, Encoding encoding)
        //{
        //    return SHA1((encoding ?? Encoding.UTF8).GetBytes(input)).ToHex();
        //}
        //static string ToHex(this byte[] data)
        //{
        //    StringBuilder s = new StringBuilder();
        //    for (int i = 0; i < data.Length; i++)
        //        s.AppendFormat("{0:X2}", data[i]);
        //    return s.ToString();
        //}

        public static string SHA1(string input, Encoding encoding = null) => SHA(input, encoding, Cryptography.SHA1.Create);
        public static string SHA256(string input, Encoding encoding = null) => SHA(input, encoding, Cryptography.SHA256.Create);
        public static string SHA384(string input, Encoding encoding = null) => SHA(input, encoding, Cryptography.SHA384.Create);
        public static string SHA512(string input, Encoding encoding = null) => SHA(input, encoding, Cryptography.SHA512.Create);

        public static byte[] SHA1(this byte[] input) => input.SHA(Cryptography.SHA1.Create);
        public static byte[] SHA256(this byte[] input) => input.SHA(Cryptography.SHA256.Create);
        public static byte[] SHA384(this byte[] input) => input.SHA(Cryptography.SHA384.Create);
        public static byte[] SHA512(this byte[] input) => input.SHA(Cryptography.SHA512.Create);

        private static byte[] SHA<T>(this byte[] input, Func<T> create) where T : HashAlgorithm
        {
            var t = create();
            return t.ComputeHash(input);
        }

        private static string SHA<T>(string input, Encoding encoding, Func<T> create) where T : HashAlgorithm
        {
            var t = create();
            var data1 = encoding.GetBytes(input);
            var data2 = t.ComputeHash(data1);
            return encoding.GetString(data2);
        }

        public static AesCryptoServiceProvider AES = new AesCryptoServiceProvider();
        public static TripleDESCryptoServiceProvider TripleDES = new TripleDESCryptoServiceProvider();
        public static DESCryptoServiceProvider DES = new DESCryptoServiceProvider();
        public static DSACryptoServiceProvider DSA = new DSACryptoServiceProvider();

        public static byte[] Encrypt<T>(this T provider, string input, string password, string salt, Encoding encoding) where T : SymmetricAlgorithm
        {
            encoding = encoding ?? Encoding.UTF8;
            return provider.Encrypt<T>(encoding.GetBytes(input), password, encoding.GetBytes(salt));
        }
        public static byte[] Encrypt<T>(this T provider, byte[] input, string password, byte[] salt) where T : SymmetricAlgorithm
        {
            Rfc2898DeriveBytes rfc = new Rfc2898DeriveBytes(password, salt);
            using (MemoryStream ms = new MemoryStream())
            using (ICryptoTransform transform = provider.CreateEncryptor(rfc.GetBytes(provider.KeySize / 8), rfc.GetBytes(provider.BlockSize / 8)))
            using (CryptoStream encryptor = new CryptoStream(ms, transform, CryptoStreamMode.Write))
            {
                encryptor.Write(input, 0, input.Length);
                encryptor.FlushFinalBlock();
                return ms.ToArray();
            }
        }

        public static string Decrypt<T>(this T provider, byte[] input, string password, string salt, Encoding encoding) where T : SymmetricAlgorithm
        {
            encoding = encoding ?? Encoding.UTF8;
            return encoding.GetString(provider.Decrypt<T>(input, password, encoding.GetBytes(salt)));
        }
        public static byte[] Decrypt<T>(this T provider, byte[] input, string password, byte[] salt) where T : SymmetricAlgorithm
        {
            Rfc2898DeriveBytes rfc = new Rfc2898DeriveBytes(password, salt);
            using (MemoryStream ms = new MemoryStream())
            using (ICryptoTransform transform = provider.CreateDecryptor(rfc.GetBytes(provider.KeySize / 8), rfc.GetBytes(provider.BlockSize / 8)))
            using (CryptoStream decryptor = new CryptoStream(ms, transform, CryptoStreamMode.Write))
            {
                decryptor.Write(input, 0, input.Length);
                decryptor.FlushFinalBlock();
                return ms.ToArray();
            }
        }



        /// <summary>
        /// CFS Hash
        /// </summary>
        /// <see cref="https://github.com/Alger23/CFS/blob/master/CFS/CFS.cs"/>
        /// <returns></returns>
        public static string CFS(this string codeStr)
        {
            var CodeLen = 30;
            var CodeSpace = 0;
            var Been = 0;

            CodeSpace = CodeLen - codeStr.Length;
            if (CodeSpace > 1)
            {
                for (var cecr = 1; cecr <= CodeSpace; cecr++)
                {
                    codeStr = codeStr + Convert.ToChar(21);
                }
            }
            double NewCode = 1;

            for (var cecb = 1; cecb <= CodeLen; cecb++)
            {
                Been = CodeLen + ((int)codeStr[cecb - 1]) * cecb;
                NewCode = NewCode * Been;
            }

            var tmpNewCode = NewCode.ToString("0.###############E+0");   //to convert to the same precision as c# code
            codeStr = tmpNewCode.ToString().ToUpper();
            var NewCode2 = "";

            for (var cec = 1; cec <= codeStr.Length; cec++)
            {
                int posStart = cec - 1;
                int len = (codeStr.Length - posStart) >= 3 ? 3 : (codeStr.Length - (cec - 1));
                NewCode2 = NewCode2 + CfsCode(codeStr.Substring(cec - 1, len));
            }

            var CfsEncodeStr = "";
            for (var cec = 20; cec <= NewCode2.Length - 18; cec += 2)
            {
                CfsEncodeStr = CfsEncodeStr + NewCode2[cec - 1];
            }
            return CfsEncodeStr.ToUpper();
        }

        private static string CfsCode(string nWord)
        {
            var num = "";
            for (var cc = 1; cc <= nWord.Length; cc++)
            {
                num += ((int)nWord[cc - 1]).ToString();
            }
            var result = Convert.ToInt64(num).ToString("X");
            return result;
        }

        #region CRC16

        const ushort crc_polynomial = 0xA001;
        static readonly ushort[] crc_table = Init_Crc16Table();

        public static ushort CRC16(byte[] bytes, int offset, int length)
        {
            ushort crc = 0;
            for (int i = offset, cnt = 0; i < bytes.Length && cnt < length; ++i, length++)
            {
                byte index = (byte)(crc ^ bytes[i]);
                crc = (ushort)((crc >> 8) ^ crc_table[index]);
            }
            return crc;
        }

        public static ushort CRC16(byte[] bytes)
        {
            ushort crc = 0;
            for (int i = 0; i < bytes.Length; ++i)
            {
                byte index = (byte)(crc ^ bytes[i]);
                crc = (ushort)((crc >> 8) ^ crc_table[index]);
            }
            return crc;
        }

        static ushort[] Init_Crc16Table()
        {
            ushort value;
            ushort temp;
            var table = new ushort[256];
            for (ushort i = 0; i < table.Length; ++i)
            {
                value = 0;
                temp = i;
                for (byte j = 0; j < 8; ++j)
                {
                    if (((value ^ temp) & 0x0001) != 0)
                    {
                        value = (ushort)((value >> 1) ^ crc_polynomial);
                    }
                    else
                    {
                        value >>= 1;
                    }
                    temp >>= 1;
                }
                table[i] = value;
            }
            return table;
        }

        public static UInt16 ModRTU_CRC(byte[] buf) => ModRTU_CRC(buf, buf.Length);
        public static UInt16 ModRTU_CRC(byte[] buf, int len)
        {
            UInt16 crc = 0xFFFF;

            for (int pos = 0; pos < len; pos++)
            {
                crc ^= (UInt16)buf[pos];          // 取出第一個byte與crc XOR

                for (int i = 8; i != 0; i--)
                {    // 巡檢每個bit  
                    if ((crc & 0x0001) != 0)
                    {      // 如果 LSB = 1   
                        crc >>= 1;                    // 右移1bit 並且 XOR 0xA001  
                        crc ^= crc_polynomial;
                    }
                    else                            // 如果 LSB != 1  
                        crc >>= 1;                    // 右移1bit
                }
            }

            return crc;

        }
        #endregion
    }
}
