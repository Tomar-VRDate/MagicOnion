using System;
using MessagePack;
using MessagePack.Formatters;

namespace MagicOnion.Server.Hubs;

[MessagePackFormatter(typeof(StreamingHubResponseMessageFormatter))]
public class StreamingHubResponseMessage
{
    public int MessageId { get; }
    public int? MethodId { get; }
    public object? Response { get; }

    public int? StatusCode { get; }
    public string? Detail { get; }
    public string? Message { get; }

    // error-response:  [messageId, statusCode, detail, StringMessage]
    public StreamingHubResponseMessage(int messageId, int statusCode, string detail, string? message)
    {
        MessageId = messageId;

        MethodId = default;
        Response = default;

        StatusCode = statusCode;
        Detail = detail;
        Message = message;
    }

    // response:  [messageId, methodId, response]
    public StreamingHubResponseMessage(int messageId, int methodId, object? response)
    {
        MessageId = messageId;

        MethodId = methodId;
        Response = response;

        StatusCode = default;
        Detail = default;
        Message = default;
    }

    // response:  [messageId, response]
    public StreamingHubResponseMessage(int messageId, object? response)
    {
        MessageId = messageId;

        MethodId = default;
        Response = response;

        StatusCode = default;
        Detail = default;
        Message = default;
    }

    public class StreamingHubResponseMessageFormatter : IMessagePackFormatter<StreamingHubResponseMessage>
    {
        public void Serialize(ref MessagePackWriter writer, StreamingHubResponseMessage value, MessagePackSerializerOptions options)
        {
            if (value.StatusCode is not null)
            {
                // error-response:  [messageId, statusCode, detail, stringMessage]
                writer.WriteArrayHeader(4);
                writer.WriteInt32(value.MessageId);
                writer.WriteInt32(value.StatusCode.Value);
                writer.Write(value.Detail);
                if (value.Message is null)
                {
                    writer.WriteNil();
                }
                else
                {
                    writer.Write(value.Message);
                }
            }
            else if (value.MethodId.HasValue)
            {
                // response: [messageId, methodId, response]
                writer.WriteArrayHeader(3);
                writer.WriteInt32(value.MessageId);
                writer.WriteInt32(value.MethodId.Value);
                if (value.Response is null or Nil)
                {
                    writer.WriteNil();
                }
                else
                {
                    MessagePackSerializer.Serialize(value.Response.GetType(), ref writer, value.Response, options);
                }
            }
            else
            {
                // response:  [messageId, response]
                writer.WriteArrayHeader(2);
                writer.WriteInt32(value.MessageId);
                if (value.Response is null or Nil)
                {
                    writer.WriteNil();
                }
                else
                {
                    MessagePackSerializer.Serialize(value.Response.GetType(), ref writer, value.Response, options);
                }
            }
            writer.Flush();
        }

        public StreamingHubResponseMessage Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
        {
            throw new NotSupportedException();
        }
    }
}