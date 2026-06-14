namespace RedisTransport.Transport;

internal static class RedisKeys
{
    public static string QueueStream(string queueName)
    {
        return $"mt:q:{queueName}";
    }

    public static string QueueNotify(string queueName)
    {
        return $"mt:q:{queueName}:notify";
    }

    public static string TopicSubscribers(string topicName)
    {
        return $"mt:subs:{topicName}";
    }
}