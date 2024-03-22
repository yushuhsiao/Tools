using System.Threading;
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

        public void Exit() => Interlocked.Exchange(ref _state, null);

        void IDisposable.Dispose() => Exit();

        public override string ToString() => IsBusy.ToString();
    }
}
