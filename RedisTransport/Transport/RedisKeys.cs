namespace RedisTransport.Transport;

internal static class RedisKeys
{
    public static string QueueStream(string queueName) => $"mt:q:{queueName}";

    public static string QueueNotify(string queueName) => $"mt:q:{queueName}:notify";

    public static string TopicSubscribers(string topicName) => $"mt:subs:{topicName}";
}
