using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Reflection;
using Grpc.Core;
using MagicOnion.Internal;
using MagicOnion.Utils;
using MessagePack;
using MessagePack.Formatters;

namespace MagicOnion.Server.Hubs;

internal static class StreamingHubMarshaller
{
    internal static Marshaller<Box<StreamingHubResponseMessage>> CreateForResponse(MethodHandler methodHandler, MessagePackSerializerOptions serializerOptions) => new Marshaller<Box<StreamingHubResponseMessage>>(
        (boxedMessage, ctx) =>
        {
            var message = boxedMessage.Value;
            Box.Return(boxedMessage);

            var writer = new MessagePackWriter(ctx.GetBufferWriter());
            if (message.StatusCode is not null)
            {
                // error-response:  [messageId, statusCode, detail, stringMessage]
                writer.WriteArrayHeader(4);
                writer.Write(message.MessageId);
                writer.Write(message.StatusCode.Value);
                writer.Write(message.Detail);
                if (message.Message is null)
                {
                    writer.WriteNil();
                }
                else
                {
                    writer.Write(message.Message);
                }
            }
            else if (message.MethodId.HasValue)
            {
                // response: [messageId, methodId, response]
                writer.WriteArrayHeader(3);
                writer.Write(message.MessageId);
                writer.Write(message.MethodId.Value);
                if (message.Response is null or Nil)
                {
                    writer.WriteNil();
                }
                else
                {
                    var handlers = StreamingHubHandlerRepository.GetHandlers(methodHandler);
                    if (handlers.TryGetValue(message.MethodId.Value, out var handler))
                    {
                        handler.Serializer.Serialize(ref writer, message.Response);
                    }
                    else
                    {
                        MessagePackSerializer.Serialize(message.Response.GetType(), ref writer, message.Response, serializerOptions);
                    }
                }
            }
            else
            {
                // response:  [messageId, response]
                writer.WriteArrayHeader(2);
                writer.Write(message.MessageId);
                if (message.Response is null or Nil)
                {
                    writer.WriteNil();
                }
                else
                {
                    MessagePackSerializer.Serialize(message.Response.GetType(), ref writer, message.Response, serializerOptions);
                }
            }

            writer.Flush();
            ctx.Complete();
        },
        (ctx) => throw new NotSupportedException()
    );


    internal static Marshaller<Box<StreamingHubRequestMessage>> CreateForRequest(MethodHandler methodHandler, MessagePackSerializerOptions serializerOptions) => new Marshaller<Box<StreamingHubRequestMessage>>(
        (message, ctx) => throw new NotSupportedException(),
        (ctx) =>
        {
            var messagePayload = ctx.PayloadAsReadOnlySequence();
            var reader = new MessagePackReader(messagePayload);
            var (methodId, messageId, offset) = FetchHeader(ref reader);

            var handlers = StreamingHubHandlerRepository.GetHandlers(methodHandler);
            if (handlers.TryGetValue(methodId, out var handler))
            {
                return Box.Create(StreamingHubRequestMessage.Create(messageId, methodId, handler.Serializer.Deserialize(ref reader), handler));
            }

            return Box.Create(StreamingHubRequestMessage.Create(messageId, methodId, default, default));

            static (int methodId, int messageId, int offset) FetchHeader(ref MessagePackReader reader)
            {
                var length = reader.ReadArrayHeader();
                if (length == 2)
                {
                    // void: [methodId, [argument]]
                    var mid = reader.ReadInt32();
                    var consumed = (int)reader.Consumed;

                    return (mid, -1, consumed);
                }
                else if (length == 3)
                {
                    // T: [messageId, methodId, [argument]]
                    var msgId = reader.ReadInt32();
                    var metId = reader.ReadInt32();
                    var consumed = (int)reader.Consumed;
                    return (metId, msgId, consumed);
                }
                else
                {
                    ThrowInvalidDataFormatException();
                    return default; // Never
                }
            }

            static void ThrowInvalidDataFormatException() => throw new InvalidOperationException("Invalid data format.");
        });
}