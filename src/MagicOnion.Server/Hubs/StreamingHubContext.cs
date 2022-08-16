using MagicOnion.Utils;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace MagicOnion.Server.Hubs
{
    public class StreamingHubContext
    {
        ConcurrentDictionary<string, object>? items;
        readonly StreamingServiceContext<StreamingHubRequestMessage, StreamingHubResponseMessage> serviceContext;

        /// <summary>Object storage per invoke.</summary>
        public ConcurrentDictionary<string, object> Items
        {
            get
            {
                lock (this) // lock per self! is this dangerous?
                {
                    if (items == null) items = new ConcurrentDictionary<string, object>();
                }
                return items;
            }
        }

        /// <summary>Raw gRPC Context.</summary>
        public ServiceContext ServiceContext => serviceContext;
        public StreamingHubHandler StreamingHubHandler { get; }
        public object HubInstance { get; }

        public object? Request { get; }
        public string Path { get; }
        public DateTime Timestamp { get;}

        // helper for reflection
        internal MessagePackSerializerOptions SerializerOptions => ServiceContext.SerializerOptions;
        public Guid ConnectionId => ServiceContext.ContextId;

        // public AsyncLock AsyncWriterLock { get; internal set; } = default!; /* lateinit */
        internal int MessageId { get; }
        internal int MethodId { get; }

        internal StreamingHubContext(
            StreamingServiceContext<StreamingHubRequestMessage, StreamingHubResponseMessage> serviceContext,
            StreamingHubHandler handler,
            object hubInstance,
            object? request,
            DateTime timestamp,
            int messageId
        )
        {
            this.serviceContext = serviceContext;
            StreamingHubHandler = handler;
            HubInstance = hubInstance;
            Request = request;
            Path = handler.ToString();
            Timestamp = timestamp;
            MessageId = messageId;
            MethodId = handler.MethodId;
        }

        // helper for reflection
        internal async ValueTask WriteResponseMessageNil(Task value)
        {
            if (MessageId == -1) // don't write.
            {
                return;
            }

            await value.ConfigureAwait(false);
            serviceContext.QueueResponseStreamWrite(StreamingHubResponseMessage.Create(MessageId, MethodId, default));
        }

        internal async ValueTask WriteResponseMessage<T>(Task<T> value)
        {
            if (MessageId == -1) // don't write.
            {
                return;
            }

            var vv = await value.ConfigureAwait(false);
            serviceContext.QueueResponseStreamWrite(StreamingHubResponseMessage.Create(MessageId, MethodId, vv));
        }

        internal ValueTask WriteErrorMessage(int statusCode, string detail, Exception? ex, bool isReturnExceptionStackTraceInErrorDetail)
        {
            var msg = (isReturnExceptionStackTraceInErrorDetail && ex != null)
                ? ex.ToString()
                : null;

            serviceContext.QueueResponseStreamWrite(StreamingHubResponseMessage.Create(MessageId, statusCode, detail, msg));
            return default;
        }
    }
}
