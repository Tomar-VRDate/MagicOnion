using System;
using MagicOnion.Internal;
using MessagePack;
using MessagePack.Formatters;

namespace MagicOnion.Server.Hubs;

public readonly struct StreamingHubResponseMessage
{
    public int MessageId { get; }
    public int? MethodId  { get; }
    public object? Response  { get; }

    public int? StatusCode  { get; }
    public string? Detail  { get; }
    public string? Message  { get; }

    internal StreamingHubResponseMessage(int messageId, int? methodId, object? response, int? statusCode, string? detail, string? message)
    {
        MessageId = messageId;
        MethodId = methodId;
        Response = response;
        StatusCode = statusCode;
        Detail = detail;
        Message = message;
    }

    // error-response:  [messageId, statusCode, detail, StringMessage]
    public static StreamingHubResponseMessage Create(int messageId, int statusCode, string detail, string? message)
        => new StreamingHubResponseMessage(messageId, default, default, statusCode, detail, message);
    // response:  [messageId, methodId, response]
    public static StreamingHubResponseMessage Create(int messageId, int methodId, object? response)
        => new StreamingHubResponseMessage(messageId, methodId, response, default, default, default);
    // response:  [messageId, response]
    public static StreamingHubResponseMessage Create(int messageId, object? response)
        => new StreamingHubResponseMessage(messageId, default, response, default, default, default);
}

