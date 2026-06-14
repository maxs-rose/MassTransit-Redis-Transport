namespace RedisTransport.Transport.Configuration;

public sealed class RedisReceiveSettings
{
    public RedisReceiveSettings(IRedisEndpointConfiguration endpointConfiguration, string queueName, TimeSpan? autoDeleteOnIdle = null)
    {
        EndpointConfiguration = endpointConfiguration;
        QueueName = queueName;
        AutoDeleteOnIdle = autoDeleteOnIdle;

        PollingInterval = TimeSpan.FromSeconds(1);
        LockDuration = TimeSpan.FromSeconds(30);
        PrefetchCount = 16;
    }

    public IRedisEndpointConfiguration EndpointConfiguration { get; }
    public string QueueName { get; set; }
    public TimeSpan? AutoDeleteOnIdle { get; set; }
    public TimeSpan PollingInterval { get; set; }
    public TimeSpan LockDuration { get; set; }
    public int PrefetchCount { get; set; }
    public int? ConcurrentMessageLimit { get; set; }
    public TimeSpan? MessageTimeToLive { get; set; }
}
