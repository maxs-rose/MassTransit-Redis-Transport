using MassTransit;

namespace RedisTransport.Transport.Configuration;

public interface IRedisReceiveEndpointConfigurator : IReceiveEndpointConfigurator
{
    public TimeSpan PollingInterval { set; }
    TimeSpan? AutoDeleteOnIdle { set; }

    TimeSpan? MessageTimeToLive { set; }
}