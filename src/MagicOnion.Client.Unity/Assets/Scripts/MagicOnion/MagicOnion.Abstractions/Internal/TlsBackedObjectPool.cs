using System;

namespace MagicOnion.Internal
{
    public class TlsBackedObjectPool<T>
    {
        [ThreadStatic]
        private static T[]? _pooled;
        [ThreadStatic]
        private static int _index;
    
        private readonly Func<T> _factory;
        private readonly int _bucketSize;
    
        public TlsBackedObjectPool(Func<T> factory, int bucketSize = 16)
        {
            _factory = factory;
            _bucketSize = bucketSize;
        }
    
        public T Rent()
        {
            if (_pooled is null || _index == 0)
            {
                var value = _factory();
                return value;
            }
            else
            {
                var value = _pooled[--_index];
                return value;
            }
        }
    
        public void Return(T value)
        {
            if (value is null)
            {
                return;
            }
            else
            {
                if (_pooled is null)
                {
                    _pooled = new T[_bucketSize];
                }
            
                if (_index != _pooled.Length)
                {
                    _pooled[_index++] = value;
                }
            }
        }
    }
}