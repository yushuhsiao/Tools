using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace System.Threading
{
    [DebuggerStepThrough]
    public class TimeCounter
    {
        long _BeginTime;

        long BeginTime
        {
            get => Interlocked.Read(ref _BeginTime);
            set => Interlocked.Exchange(ref _BeginTime, value);
        }

        public long ElapsedTicks => DateTime.Now.Ticks - this.BeginTime;

        public DateTime Time => new DateTime(this.BeginTime);

        public TimeCounter(bool reset = true)
        {
            if (reset) this.Reset();
        }

        public void Reset() => this.BeginTime = DateTime.Now.Ticks;

        public void ClearTicks()
        {
            this.BeginTime = 0;
        }

        public void SetTicks(double milliseconds)
        {
            if (milliseconds >= 0)
                this.BeginTime = DateTime.Now.AddMilliseconds(milliseconds).Ticks;
        }

        public bool IsTimeout(double milliseconds, bool reset = false)
        {
            if (milliseconds < 0) return false;
            TimeSpan _t1 = TimeSpan.FromMilliseconds(milliseconds);
            long t1 = _t1.Ticks;
            long t2 = this.BeginTime;
            long t3 = DateTime.Now.Ticks - t2;
            if (t2 == 0 || t1 < t3)
            {
                if (reset)
                    this.Reset();
                return true;
            }
            return false;
        }

        public bool Timeout(double milliseconds, Action cb, bool reset = true)
        {
            if (Enabled && IsTimeout(milliseconds))
            {
                if (Interlocked.CompareExchange(ref this.busy, this, null) == null)
                {
                    if (reset) this.Reset();
                    try { cb?.Invoke(); }
                    finally { Interlocked.Exchange(ref this.busy, null); }
                    return true;
                }
            }
            return false;
        }

        public IEnumerable<TimeCounter> Timeout(double milliseconds, bool reset = true)
        {
            if (Enabled && IsTimeout(milliseconds))
            {
                if (Interlocked.CompareExchange(ref this.busy, this, null) == null)
                {
                    try
                    {
                        yield return this;
                        if (reset) this.Reset();
                    }
                    finally { Interlocked.Exchange(ref this.busy, null); }
                }
            }
        }

        int enabled = 1;
        public bool Enabled
        {
            get => Interlocked.CompareExchange(ref this.enabled, 0, 0) != 0;
            set => Interlocked.Exchange(ref this.enabled, value ? 1 : 0);
        }

        object busy;
        //public bool TimeoutProc(double milliseconds, Action cb, bool reset = true)
        //{
        //    foreach (var n in Timeout(milliseconds, reset))
        //    {
        //        cb?.Invoke();
        //        return true;
        //    }
        //    return false;
        //    //if (IsTimeout(milliseconds))
        //    //{
        //    //    if (Interlocked.CompareExchange(ref this.busy, this, null) == null)
        //    //    {
        //    //        if (reset) this.Reset();
        //    //        try { cb?.Invoke(); }
        //    //        finally { Interlocked.Exchange(ref this.busy, null); }
        //    //        return true;
        //    //    }
        //    //}
        //    //return false;
        //}

        //object tick_milliseconds;
        //Tick.Handler tick_cb;
        //object tick_reset;
        //public void TimeoutProc_Tick(double milliseconds, Tick.Handler cb, bool reset = true)
        //{
        //    if (Interlocked.CompareExchange(ref this.tick_cb, cb, null) == null)
        //    {
        //        Interlocked.Exchange(ref tick_milliseconds, milliseconds);
        //        Interlocked.Exchange(ref tick_reset, reset);
        //        Tick.OnTick += Tick1;
        //    }
        //}

        //bool Tick1()
        //{
        //    double milliseconds = (double)Interlocked.CompareExchange(ref tick_milliseconds, null, null);
        //    bool reset = (bool)Interlocked.CompareExchange(ref tick_reset, null, null);
        //    this.TimeoutProc(milliseconds, Tick2, reset);
        //    return Interlocked.CompareExchange(ref tick_cb, null, null) != null;
        //}

        //void Tick2()
        //{
        //    Tick.Handler cb = Interlocked.CompareExchange(ref tick_cb, null, null);
        //    if (cb == null) return;
        //    if (cb()) return;
        //    Interlocked.Exchange(ref cb, null);
        //}
    }
}