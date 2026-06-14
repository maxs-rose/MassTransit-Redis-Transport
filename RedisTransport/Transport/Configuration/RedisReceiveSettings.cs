namespace RedisTransport.Transport.Configuration;

internal sealed record RedisReceiveSettings(string QueueName, TimeSpan? AutoDeleteOnIdle = null)
{
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(30);
    public int PrefetchCount { get; set; } = 16;
    public TimeSpan? MessageTimeToLive { get; set; }
    public TimeSpan? AutoDeleteOnIdle { get; set; } = AutoDeleteOnIdle;
}