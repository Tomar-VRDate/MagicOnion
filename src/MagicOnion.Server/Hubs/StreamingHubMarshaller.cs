using System;
using System.Buffers;
using Grpc.Core;
using MagicOnion.Utils;
using MessagePack;

namespace MagicOnion.Server.Hubs;

internal static class StreamingHubMarshaller
{
    internal static Marshaller<StreamingHubResponseMessage> CreateForResponse(MessagePackSerializerOptions serializerOptions) => new Marshaller<StreamingHubResponseMessage>(
        (message, ctx) =>
        {
            MessagePackSerializer.Serialize(ctx.GetBufferWriter(), message, serializerOptions);
            ctx.Complete();
        },
        (ctx) => throw new NotSupportedException()
    );

    internal static Marshaller<StreamingHubRequestMessage> CreateForRequest(MethodHandler methodHandler, MessagePackSerializerOptions serializerOptions) => new Marshaller<StreamingHubRequestMessage>(
        (message, ctx) => throw new NotSupportedException(),
        (ctx) =>
        {
            var messagePayload = ctx.PayloadAsReadOnlySequence();
            var (methodId, messageId, offset) = FetchHeader(messagePayload);

            var handlers = StreamingHubHandlerRepository.GetHandlers(methodHandler);
            if (handlers.TryGetValue(methodId, out var handler))
            {
                return new StreamingHubRequestMessage(messageId, methodId, MessagePackSerializer.Deserialize(handler.RequestType, messagePayload.Slice(offset), serializerOptions), handler);
            }

            return new StreamingHubRequestMessage(messageId, methodId, default, default);

            static (int methodId, int messageId, int offset) FetchHeader(ReadOnlySequence<byte> msgData)
            {
                var messagePackReader = new MessagePackReader(msgData);

                var length = messagePackReader.ReadArrayHeader();
                if (length == 2)
                {
                    // void: [methodId, [argument]]
                    var mid = messagePackReader.ReadInt32();
                    var consumed = (int)messagePackReader.Consumed;

                    return (mid, -1, consumed);
                }
                else if (length == 3)
                {
                    // T: [messageId, methodId, [argument]]
                    var msgId = messagePackReader.ReadInt32();
                    var metId = messagePackReader.ReadInt32();
                    var consumed = (int)messagePackReader.Consumed;
                    return (metId, msgId, consumed);
                }
                else
                {
                    throw new InvalidOperationException("Invalid data format.");
                }
            }
        });
}