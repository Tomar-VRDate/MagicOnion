using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace MagicOnion.Internal
{
    internal static class UnboxAsyncStreamReader
    {
        public static IAsyncStreamReader<T> Create<T, TRaw>(IAsyncStreamReader<TRaw> rawStreamReader)
            => (typeof(TRaw) == typeof(Box<T>)) ? new UnboxAsyncStreamReader<T>((IAsyncStreamReader<Box<T>>)rawStreamReader) : (IAsyncStreamReader<T>)rawStreamReader;
    }

    internal class UnboxAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        readonly IAsyncStreamReader<Box<T>> inner;
        T current;
        bool unboxed;

        public UnboxAsyncStreamReader(IAsyncStreamReader<Box<T>> inner)
        {
            this.inner = inner;
        }

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            unboxed = false;
            return inner.MoveNext(cancellationToken);
        }

        public T Current
        {
            get
            {
                if (unboxed)
                {
                    return current;
                }

                current = inner.Current.Value;
                Box.Return(inner.Current);
                unboxed = true;

                return current;
            }
        }
    }
}