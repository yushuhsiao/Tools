using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace System
{
    public struct UnixTimeStamp
    {
        public static long tick_base = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        public static long tick_ss = new TimeSpan(0, 0, 1).Ticks;
        public static long tick_ms = new TimeSpan(0, 0, 0, 0, 1).Ticks;

        long value;
        public long Millisecond
        {
            get { return this.value; }
            set { this.value = value; }
        }
        public long Second
        {
            get
            {
                long value = this.value;
                value /= 1000;
                return value;
            }
            set
            {
                this.value = value;
                this.value *= 1000;
            }
        }

        public UnixTimeStamp(long timestamp)
            : this()
        {
            this.value = timestamp;
        }

        public static explicit operator long(UnixTimeStamp t)
        {
            return t.value;
        }
        public static implicit operator UnixTimeStamp(long t)
        {
            return new UnixTimeStamp() { value = t };
        }

        public static explicit operator DateTime(UnixTimeStamp t)
        {
            long value = t.value;
            value *= tick_ms;
            value += tick_base;
            return new DateTime(value, DateTimeKind.Utc).ToLocalTime();
        }
        public static implicit operator UnixTimeStamp(DateTime t)
        {
            long value = t.ToUniversalTime().Ticks;
            value -= tick_base;
            value /= tick_ms;
            return new UnixTimeStamp() { value = value };
        }
    }
    public static partial class _DateTimeExtensions
    {
        private static readonly DateTime UnixBaseTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static long unix_time_base_tick = UnixBaseTime.Ticks;

        public static long ToUnixTime(this DateTime t) => (t.Ticks - unix_time_base_tick) / 10000;
        public static DateTime FromUnixTime(long unixtime) => new DateTime((unixtime + unix_time_base_tick) * 10000);
    }
}
