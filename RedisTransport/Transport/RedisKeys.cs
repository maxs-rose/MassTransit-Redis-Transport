namespace RedisTransport.Transport;

internal static class RedisKeys
{
    internal static string KeyPrefix { get; set; } = string.Empty;

    public static string QueueStream(string queueName)
    {
        return WithPrefix($"mt:q:{queueName}");
    }

    public static string QueueNotify(string queueName)
    {
        return WithPrefix($"mt:q:{queueName}:notify");
    }

    public static string TopicSubscribers(string topicName)
    {
        return WithPrefix($"mt:subs:{topicName}");
    }

    private static string WithPrefix(string key)
    {
        return $"{KeyPrefix}{key}";
    }
}