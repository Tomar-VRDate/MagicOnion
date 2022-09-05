using Grpc.Core;
using MessagePack;
using System;
using System.IO;
using System.Threading.Tasks;
using MagicOnion.Internal;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Http.Features;

namespace MagicOnion.Server.Hubs
{
    public abstract class StreamingHubBase<THubInterface, TReceiver> : ServiceBase<THubInterface>, IStreamingHub<THubInterface, TReceiver>
        where THubInterface : IStreamingHub<THubInterface, TReceiver>
    {
        static readonly Metadata ResponseHeaders = new Metadata()
        {
            { "x-magiconion-streaminghub-version", "2" },
        };

        public HubGroupRepository Group { get; private set; } = default!; /* lateinit */

        internal StreamingServiceContext<StreamingHubRequestMessage, StreamingHubResponseMessage> StreamingServiceContext
            => (StreamingServiceContext<StreamingHubRequestMessage, StreamingHubResponseMessage>)Context;

        protected Guid ConnectionId
            => Context.ContextId;

        // Broadcast Commands

        [Ignore]
        protected TReceiver Broadcast(IGroup group)
        {
            var type = DynamicBroadcasterBuilder<TReceiver>.BroadcasterType;
            return (TReceiver)Activator.CreateInstance(type, group)!;
        }

        [Ignore]
        protected TReceiver BroadcastExceptSelf(IGroup group)
        {
            return BroadcastExcept(group, Context.ContextId);
        }

        [Ignore]
        protected TReceiver BroadcastExcept(IGroup group, Guid except)
        {
            var type = DynamicBroadcasterBuilder<TReceiver>.BroadcasterType_ExceptOne;
            return (TReceiver)Activator.CreateInstance(type, new object[] { group, except })!;
        }

        [Ignore]
        protected TReceiver BroadcastExcept(IGroup group, Guid[] excepts)
        {
            var type = DynamicBroadcasterBuilder<TReceiver>.BroadcasterType_ExceptMany;
            return (TReceiver)Activator.CreateInstance(type, new object[] { group, excepts })!;
        }

        [Ignore]
        protected TReceiver BroadcastToSelf(IGroup group)
        {
            return BroadcastTo(group, Context.ContextId);
        }

        [Ignore]
        protected TReceiver BroadcastTo(IGroup group, Guid toConnectionId)
        {
            var type = DynamicBroadcasterBuilder<TReceiver>.BroadcasterType_ToOne;
            return (TReceiver)Activator.CreateInstance(type, new object[] { group, toConnectionId })!;
        }

        [Ignore]
        protected TReceiver BroadcastTo(IGroup group, Guid[] toConnectionIds)
        {
            var type = DynamicBroadcasterBuilder<TReceiver>.BroadcasterType_ToMany;
            return (TReceiver)Activator.CreateInstance(type, new object[] { group, toConnectionIds })!;
        }

        /// <summary>
        /// Called before connect, instead of constructor.
        /// </summary>
        protected virtual ValueTask OnConnecting()
        {
            return default;
        }

        /// <summary>
        /// Called after disconnect.
        /// </summary>
        protected virtual ValueTask OnDisconnected()
        {
            return default;
        }

        public async Task<DuplexStreamingResult<StreamingHubRequestMessage, StreamingHubResponseMessage>> Connect()
        {
            var streamingContext = GetDuplexStreamingContext<StreamingHubRequestMessage, StreamingHubResponseMessage>();

            var group = StreamingHubHandlerRepository.GetGroupRepository(Context.MethodHandler);
            this.Group = new HubGroupRepository(this.Context, group);
            try
            {
                await OnConnecting();
                await HandleMessageAsync();
            }
            catch (OperationCanceledException)
            {
                // NOTE: If DuplexStreaming is disconnected by the client, OperationCanceledException will be thrown.
                //       However, such behavior is expected. the exception can be ignored.
            }
            catch (IOException ex) when (ex.InnerException is ConnectionAbortedException)
            {
                // NOTE: If DuplexStreaming is disconnected by the client, IOException will be thrown.
                //       However, such behavior is expected. the exception can be ignored.
            }
            catch (Exception ex) when (ex is IOException or InvalidOperationException)
            {
                var httpRequestLifetimeFeature = this.Context.CallContext.GetHttpContext()?.Features.Get<IHttpRequestLifetimeFeature>();

                // NOTE: If the connection is completed when a message is written, PipeWriter throws an InvalidOperationException.
                // NOTE: If the connection is closed with STREAM_RST, PipeReader throws an IOException.
                //       However, such behavior is expected. the exception can be ignored.
                //       https://github.com/dotnet/aspnetcore/blob/v6.0.0/src/Servers/Kestrel/Core/src/Internal/Http2/Http2Stream.cs#L516-L523
                if (httpRequestLifetimeFeature is null || httpRequestLifetimeFeature.RequestAborted.IsCancellationRequested is false)
                {
                    throw;
                }
            }
            finally
            {
                StreamingServiceContext.CompleteStreamingHub();
                await OnDisconnected();
                await this.Group.DisposeAsync();
            }

            return streamingContext.Result();
        }

        async Task HandleMessageAsync()
        {
            var ct = Context.CallContext.CancellationToken;
            var reader = StreamingServiceContext.RequestStream!;
            var writer = StreamingServiceContext.ResponseStream!;

            // Send a hint to the client to start sending messages.
            // The client can read the response headers before any StreamingHub's message.
            await Context.CallContext.WriteResponseHeadersAsync(ResponseHeaders);

            // Write a marker that is the beginning of the stream.
            // NOTE: To prevent buffering by AWS ALB or reverse-proxy.
            // response:  [messageId, methodId, response]
            // HACK: If the ID of the message is `-1`, the client will ignore the message.
            await writer.WriteAsync(StreamingHubResponseMessage.Create(-1, 0, Nil.Default));

            // Main loop of StreamingHub.
            // Be careful to allocation and performance.
            while (await reader.MoveNext(ct)) // must keep SyncContext.
            {
                var data = reader.Current;
                if (data.Handler is null)
                {
                    throw new InvalidOperationException("Handler not found in received methodId, methodId:" + data.MethodId);
                }

                // Create a context per invoking method.
                var context = new StreamingHubContext(
                    (StreamingServiceContext<StreamingHubRequestMessage, StreamingHubResponseMessage>)Context,
                    data.Handler,
                    this,
                    data.Argument,
                    DateTime.UtcNow,
                    data.MessageId
                );

                var isErrorOrInterrupted = false;
                Context.MethodHandler.logger.BeginInvokeHubMethod(context, context.Request, data.Handler.RequestType);
                try
                {
                    await data.Handler.MethodBody.Invoke(context);
                }
                catch (ReturnStatusException ex)
                {
                    await context.WriteErrorMessage((int)ex.StatusCode, ex.Detail, null, false);
                }
                catch (Exception ex)
                {
                    isErrorOrInterrupted = true;
                    Context.MethodHandler.logger.Error(ex, context);
                    await context.WriteErrorMessage((int)StatusCode.Internal, $"An error occurred while processing handler '{data.Handler.ToString()}'.", ex, Context.MethodHandler.isReturnExceptionStackTraceInErrorDetail);
                }
                finally
                {
                    Context.MethodHandler.logger.EndInvokeHubMethod(context, (DateTime.UtcNow - context.Timestamp).TotalMilliseconds, isErrorOrInterrupted);
                }
            }
        }

        // Interface methods for Client

        THubInterface IStreamingHub<THubInterface, TReceiver>.FireAndForget()
            => throw new NotSupportedException("Invoke from client proxy only");

        Task IStreamingHub<THubInterface, TReceiver>.DisposeAsync()
            => throw new NotSupportedException("Invoke from client proxy only");

        Task IStreamingHub<THubInterface, TReceiver>.WaitForDisconnect()
            => throw new NotSupportedException("Invoke from client proxy only");
    }
}
