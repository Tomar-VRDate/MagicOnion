using System;
using System.ComponentModel;

namespace MagicOnion.Internal
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class TlsBackedObjectPool<T>
        where T : class
    {
        [ThreadStatic]
        static T[] pooled;
        [ThreadStatic]
        static int index;
    
        readonly Func<T> _factory;
        readonly int poolSize;
    
        public TlsBackedObjectPool(Func<T> factory, int poolSize = 16)
        {
            _factory = factory;
            this.poolSize = poolSize;
        }
    
        public T Rent()
        {
            if (pooled is null || index == 0)
            {
                var value = _factory();
                return value;
            }
            else
            {
                var value = pooled[--index];
                return value;
            }
        }
    
        public void Return(T value)
        {
            if (value is null)
            {
                return;
            }

            if (pooled is null)
            {
                pooled = new T[poolSize];
            }

            if (index != pooled.Length)
            {
                pooled[index++] = value;
            }
        }
    }
}