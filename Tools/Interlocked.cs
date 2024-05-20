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

        public T Exchange(T value) => Interlocked.Exchange(ref _value, value);

        public bool TrySet(T value) => CompareExchange(value, null) == null;

        public bool GetValue(out T value)
        {
            value = this.Value;
            return value != null;
        }

        void IDisposable.Dispose() => Value = null;
    }

    public class Interlocked_Bool
    {
        private object _value;

        public Interlocked_Bool(bool value) { this.Value = value; }
        public Interlocked_Bool() : this(false) { }

        public bool Value
        {
            get => Interlocked.CompareExchange(ref _value, null, null) != null;
            set => Exchange(value);
        }
        public bool Exchange(bool value) => Interlocked.Exchange(ref _value, value ? this : null) != null;
   
        public static implicit operator bool(Interlocked_Bool obj) => obj.Value;

        public override string ToString() => Value.ToString();
    }


    public class Interlocked_Int32
    {
        private int _value;

        public Interlocked_Int32(int value) { Interlocked.Exchange(ref _value, value); }
        public Interlocked_Int32() : this(0) { }

        public int Value
        {
            get => Interlocked.CompareExchange(ref _value, 0, 0);
            set => Interlocked.Exchange(ref _value, value);
        }
        public int Exchange(int value) => Interlocked.Exchange(ref _value, value);
        public int CompareExchange(int value, int comparand) => Interlocked.CompareExchange(ref _value, value, comparand);
        public int Increment => Interlocked.Increment(ref _value);
        public int Decrement => Interlocked.Decrement(ref _value);
        public int Add(int value) => Interlocked.Add(ref _value, value);
    }

    public class Interlocked_Int64
    {
        private long _value;

        public Interlocked_Int64(long value) { Interlocked.Exchange(ref _value, value); }
        public Interlocked_Int64() : this(0) { }

        public long Value
        {
            get => Interlocked.CompareExchange(ref _value, 0, 0);
            set => Interlocked.Exchange(ref _value, value);
        }
        public long Exchange(long value) => Interlocked.Exchange(ref _value, value);
        public long CompareExchange(long value, long comparand) => Interlocked.CompareExchange(ref _value, value, comparand);
        public long Increment => Interlocked.Increment(ref _value);
        public long Decrement => Interlocked.Decrement(ref _value);
        public long Add(long value) => Interlocked.Add(ref _value, value);
        public long Read() => Interlocked.Read(ref _value);
    }

    public class Interlocked_Single
    {
        private Single _value;

        public Interlocked_Single(Single value) { Interlocked.Exchange(ref _value, value); }
        public Interlocked_Single() : this(0) { }

        public Single Value
        {
            get => Interlocked.CompareExchange(ref _value, 0, 0);
            set => Interlocked.Exchange(ref _value, value);
        }
        public Single Exchange(Single value) => Interlocked.Exchange(ref _value, value);
        public Single CompareExchange(Single value, long comparand) => Interlocked.CompareExchange(ref _value, value, comparand);
    }

    public class Interlocked_Double
    {
        private Double _value;

        public Interlocked_Double(Double value) { Interlocked.Exchange(ref _value, value); }
        public Interlocked_Double() : this(0) { }

        public Double Value
        {
            get => Interlocked.CompareExchange(ref _value, 0, 0);
            set => Interlocked.Exchange(ref _value, value);
        }
        public Double Exchange(Double value) => Interlocked.Exchange(ref _value, value);
        public Double CompareExchange(Double value, long comparand) => Interlocked.CompareExchange(ref _value, value, comparand);
    }

    public static class InterlockedExtensions
    {
        public static int Increment(this Interlocked_Int32 n) => n.Increment;
        public static int Decrement(this Interlocked_Int32 n) => n.Decrement;
        public static long Increment(this Interlocked_Int64 n) => n.Increment;
        public static long Decrement(this Interlocked_Int64 n) => n.Decrement;
    }
}