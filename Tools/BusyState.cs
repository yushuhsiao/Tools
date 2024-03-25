using System.Threading;
using System.Threading.Tasks;
namespace System
{
    public class BusyState : IDisposable
    {
        private object _state;

        public static implicit operator bool(BusyState d) => d.IsBusy;

        public bool IsBusy => Interlocked.CompareExchange(ref _state, null, null) != null;
        public bool IsNotBusy => Interlocked.CompareExchange(ref _state, null, null) == null;

        public IDisposable Enter(out bool busy)
        {
            if (Interlocked.CompareExchange(ref _state, 1, null) == null)
            {
                busy = false;
                return this;
            }
            busy = true;
            return null;
        }

        public IDisposable Enter(int sleep)
        {
            for (; ; Thread.Sleep(sleep))
            {
                this.Enter(out bool busy);
                if (busy) return this;
            }
        }

        public async Task<IDisposable> EnterAsync(int sleep)
        {
            for (; ; await Task.Delay(sleep))
            {
                this.Enter(out bool busy);
                if (busy) return this;
            }
        }

        public void WaitExit(int sleep = 1)
        {
            while (this.IsBusy)
                Thread.Sleep(sleep);
        }

        public async Task WaitExitAsync(int sleep = 1)
        {
            while (this.IsBusy)
                await Task.Delay(sleep);
        }

        public void Exit() => Interlocked.Exchange(ref _state, null);

        void IDisposable.Dispose() => Exit();

        public override string ToString() => IsBusy.ToString();
    }
}