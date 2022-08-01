namespace MagicOnion.Server.Hubs;

public class StreamingHubRequestMessage
{
    public int MessageId { get; }
    public int MethodId { get; }
    public object? Argument { get; }
    public StreamingHubHandler? Handler { get; }

    public StreamingHubRequestMessage(int messageId, int methodId, object? argument, StreamingHubHandler? handler)
    {
        MessageId = messageId;
        MethodId = methodId;
        Argument = argument;
        Handler = handler;
    }
}