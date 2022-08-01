using Grpc.Core;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using MagicOnion.Server.Hubs;

namespace MagicOnion.Server
{
    internal interface IServiceContextWithRequestStream<T> : IStreamingServiceContext
    {
        IAsyncStreamReader<T>? RequestStream { get; }
    }

    internal interface IServiceContextWithResponseStream<T> : IStreamingServiceContext
    {
        IServerStreamWriter<T>? ResponseStream { get; }
        void QueueResponseStreamWrite(in T value);
    }

    public interface IServiceContext
    {
        Guid ContextId { get; }

        DateTime Timestamp { get; }

        Type ServiceType { get; }

        MethodInfo MethodInfo { get; }

        /// <summary>Cached Attributes both service and method.</summary>
        ILookup<Type, Attribute> AttributeLookup { get; }

        MethodType MethodType { get; }

        /// <summary>Raw gRPC Context.</summary>
        ServerCallContext CallContext { get; }

        MessagePackSerializerOptions SerializerOptions { get; }

        IServiceProvider ServiceProvider { get; }
    }

    public interface IStreamingServiceContext : IServiceContext
    {
        bool IsDisconnected { get; }
        void CompleteStreamingHub();
    }

    internal class StreamingServiceContext<TRequest, TResponse> : ServiceContext, IServiceContextWithRequestStream<TRequest>, IServiceContextWithResponseStream<TResponse>
    {
        readonly Lazy<QueuedResponseWriter<TResponse>> streamingResponseWriter;

        public IAsyncStreamReader<TRequest>? RequestStream { get; }
        public IServerStreamWriter<TResponse>? ResponseStream { get; }

        // used in StreamingHub
        public bool IsDisconnected { get; private set; }

        public StreamingServiceContext(
            Type serviceType,
            MethodInfo methodInfo,
            ILookup<Type, Attribute> attributeLookup,
            MethodType methodType,
            ServerCallContext context,
            MessagePackSerializerOptions serializerOptions,
            IMagicOnionLogger logger,
            MethodHandler methodHandler,
            IServiceProvider serviceProvider,
            IAsyncStreamReader<TRequest>? requestStream,
            IServerStreamWriter<TResponse>? responseStream
        ) : base(serviceType, methodInfo, attributeLookup, methodType, context, serializerOptions, logger, methodHandler, serviceProvider)
        {
            RequestStream = requestStream;
            ResponseStream = responseStream;

            // streaming hub
            if (methodType == MethodType.DuplexStreaming)
            {
                this.streamingResponseWriter = new Lazy<QueuedResponseWriter<TResponse>>(() => new QueuedResponseWriter<TResponse>(this));
            }
            else
            {
                this.streamingResponseWriter = null!;
            }
        }
        
        // used in StreamingHub
        public void QueueResponseStreamWrite(in TResponse value)
        {
            streamingResponseWriter.Value.Write(value);
        }

        public void CompleteStreamingHub()
        {
            IsDisconnected = true;
            streamingResponseWriter.Value.Dispose();
        }
    }

    public class ServiceContext : IServiceContext
    {
        internal static AsyncLocal<ServiceContext?> currentServiceContext = new AsyncLocal<ServiceContext?>();

        /// <summary>
        /// Get Current ServiceContext. This property requires to MagicOnionOptions.Enable
        /// </summary>
        public static ServiceContext? Current => currentServiceContext.Value;

        object? request;
        ConcurrentDictionary<string, object>? items;

        public Guid ContextId { get; }

        public DateTime Timestamp { get; }

        public Type ServiceType { get; }

        public MethodInfo MethodInfo { get; }

        /// <summary>Cached Attributes both service and method.</summary>
        public ILookup<Type, Attribute> AttributeLookup { get; }

        public MethodType MethodType { get; }

        /// <summary>Raw gRPC Context.</summary>
        public ServerCallContext CallContext { get; }

        public MessagePackSerializerOptions SerializerOptions { get; }

        public IServiceProvider ServiceProvider { get; }

        /// <summary>Object storage per invoke.</summary>
        public ConcurrentDictionary<string, object> Items
        {
            get
            {
                lock (CallContext) // lock per CallContext, is this dangerous?
                {
                    if (items == null) items = new ConcurrentDictionary<string, object>();
                }
                return items;
            }
        }

        internal object? Request => request;

        internal object? Result { get; set; }
        internal IMagicOnionLogger MagicOnionLogger { get; }
        internal MethodHandler MethodHandler { get; }

        public ServiceContext(
            Type serviceType,
            MethodInfo methodInfo,
            ILookup<Type, Attribute> attributeLookup,
            MethodType methodType,
            ServerCallContext context,
            MessagePackSerializerOptions serializerOptions,
            IMagicOnionLogger logger,
            MethodHandler methodHandler,
            IServiceProvider serviceProvider
        )
        {
            this.ContextId = Guid.NewGuid();
            this.ServiceType = serviceType;
            this.MethodInfo = methodInfo;
            this.AttributeLookup = attributeLookup;
            this.MethodType = methodType;
            this.CallContext = context;
            this.Timestamp = DateTime.UtcNow;
            this.SerializerOptions = serializerOptions;
            this.MagicOnionLogger = logger;
            this.MethodHandler = methodHandler;
            this.ServiceProvider = serviceProvider;
        }

        /// <summary>Get Raw Request.</summary>
        public object? GetRawRequest()
        {
            return request;
        }

        /// <summary>Set Raw Request, you can set before method body was called.</summary>
        public void SetRawRequest(object? request)
        {
            this.request = request;
        }

        /// <summary>Can get after method body was finished.</summary>
        public object? GetRawResponse()
        {
            return Result;
        }

        /// <summary>Can set after method body was finished.</summary>
        public void SetRawResponse(object? response)
        {
            Result = response;
        }
    }

    public class ClientStreamingContext<TRequest, TResponse> : IAsyncStreamReader<TRequest>, IDisposable
    {
        readonly ServiceContext context;
        readonly IAsyncStreamReader<TRequest> inner;
        readonly IMagicOnionLogger logger;

        internal ClientStreamingContext(StreamingServiceContext<TRequest, Nil /* Dummy */> context)
        {
            this.context = context;
            this.inner = context.RequestStream!;
            this.logger = context.MagicOnionLogger;
        }

        public ServiceContext ServiceContext => context;

        public TRequest Current { get; private set; } = default!; /* lateinit */

        public async Task<bool> MoveNext(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (await inner.MoveNext(cancellationToken))
            {
                this.Current = inner.Current;
                return true;
            }
            else
            {
                logger.ReadFromStream(context, true);
                return false;
            }
        }

        public void Dispose()
        {
            (inner as IDisposable)?.Dispose();
        }

        public async Task ForEachAsync(Action<TRequest> action)
        {
            while (await MoveNext(CancellationToken.None)) // ClientResponseStream is not supported CancellationToken.
            {
                action(Current);
            }
        }

        public async Task ForEachAsync(Func<TRequest, Task> asyncAction)
        {
            while (await MoveNext(CancellationToken.None))
            {
                await asyncAction(Current);
            }
        }

        public ClientStreamingResult<TRequest, TResponse> Result(TResponse result)
        {
            return new ClientStreamingResult<TRequest, TResponse>(result);
        }

        public ClientStreamingResult<TRequest, TResponse> ReturnStatus(StatusCode statusCode, string detail)
        {
            context.CallContext.Status = new Status(statusCode, detail);

            return default(ClientStreamingResult<TRequest, TResponse>); // dummy
        }
    }

    public class ServerStreamingContext<TResponse> : IAsyncStreamWriter<TResponse>
    {
        readonly ServiceContext context;
        readonly IAsyncStreamWriter<TResponse> inner;
        readonly IMagicOnionLogger logger;

        internal ServerStreamingContext(StreamingServiceContext<Nil /* Dummy */, TResponse> context)
        {
            this.context = context;
            this.inner = context.ResponseStream!;
            this.logger = context.MagicOnionLogger;
        }

        public ServiceContext ServiceContext => context;

        public WriteOptions WriteOptions
        {
            get => inner.WriteOptions;
            set => inner.WriteOptions = value;
        }

        public Task WriteAsync(TResponse message)
        {
            logger.WriteToStream(context);
            return inner.WriteAsync(message);
        }

        public ServerStreamingResult<TResponse> Result()
        {
            return default(ServerStreamingResult<TResponse>); // dummy
        }

        public ServerStreamingResult<TResponse> ReturnStatus(StatusCode statusCode, string detail)
        {
            context.CallContext.Status = new Status(statusCode, detail);

            return default(ServerStreamingResult<TResponse>); // dummy
        }
    }

    public class DuplexStreamingContext<TRequest, TResponse> : IAsyncStreamReader<TRequest>, IServerStreamWriter<TResponse>, IDisposable
    {
        readonly ServiceContext context;
        readonly IAsyncStreamReader<TRequest> innerReader;
        readonly IAsyncStreamWriter<TResponse> innerWriter;
        readonly IMagicOnionLogger logger;

        internal DuplexStreamingContext(StreamingServiceContext<TRequest, TResponse> context)
        {
            this.context = context;
            this.innerReader = context.RequestStream!;
            this.innerWriter = context.ResponseStream!;
            this.logger = context.MagicOnionLogger;
        }

        public ServiceContext ServiceContext => context;
        
        /// <summary>
        /// IServerStreamWriter Methods.
        /// </summary>
        public WriteOptions WriteOptions
        {
            get => innerWriter.WriteOptions;
            set => innerWriter.WriteOptions = value;
        }

        /// <summary>IAsyncStreamReader Methods.</summary>
        public TRequest Current { get; private set; } = default!; /* lateinit */

        /// <summary>IAsyncStreamReader Methods.</summary>
        public async Task<bool> MoveNext(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (await innerReader.MoveNext(cancellationToken))
            {
                logger.ReadFromStream(context, false);
                this.Current = innerReader.Current;
                return true;
            }
            else
            {
                logger.ReadFromStream(context, true);
                return false;
            }
        }

        /// <summary>IAsyncStreamReader Methods.</summary>
        public void Dispose()
        {
            (innerReader as IDisposable)?.Dispose();
        }

        /// <summary>
        /// IServerStreamWriter Methods.
        /// </summary>
        public Task WriteAsync(TResponse message)
        {
            logger.WriteToStream(context);
            return innerWriter.WriteAsync(message);
        }

        public DuplexStreamingResult<TRequest, TResponse> Result()
        {
            return default(DuplexStreamingResult<TRequest, TResponse>); // dummy
        }

        public DuplexStreamingResult<TRequest, TResponse> ReturnStatus(StatusCode statusCode, string detail)
        {
            context.CallContext.Status = new Status(statusCode, detail);

            return default(DuplexStreamingResult<TRequest, TResponse>); // dummy
        }
    }
}