namespace System.Threading
{
    public class Interlocked<T> : IDisposable where T : class
    {
        private T _value;

        public static implicit operator T(Interlocked<T> obj) => obj._value;

        public bool IsNull => Value == null;
    
        public bool IsNotNull => Value != null;

        public T Value
        {
            get => Interlocked.CompareExchange(ref _value, null, null);
            set => Interlocked.Exchange(ref _value, value);
        }

        public T CompareExchange(T value, T comparand) => Interlocked.CompareExchange(ref _value, value, comparand);

        public bool TrySet(T value) => CompareExchange(value, null) == null;

        public bool GetValue(out T value)
        {
            value = this.Value;
            return value != null;
        }

        void IDisposable.Dispose() => Value = null;
    }
}