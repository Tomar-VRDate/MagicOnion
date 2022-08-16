using MagicOnion.Internal;
using System;

namespace MagicOnion.Server.Hubs;

public readonly struct StreamingHubRequestMessage
{
    public int MessageId { get; }
    public int MethodId { get; }
    public object? Argument { get; }

    public StreamingHubHandler? Handler { get; }

    private StreamingHubRequestMessage(int messageId, int methodId, object? argument, StreamingHubHandler? handler)
    {
        MessageId = messageId;
        MethodId = methodId;
        Argument = argument;
        Handler = handler;
    }

    public static StreamingHubRequestMessage Create(int messageId, int methodId, object? argument, StreamingHubHandler? handler)
        => new StreamingHubRequestMessage(messageId, methodId, argument, handler);
}