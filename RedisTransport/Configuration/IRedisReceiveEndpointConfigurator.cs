using MassTransit;

namespace RedisTransport.Configuration;

public interface IRedisReceiveEndpointConfigurator : IReceiveEndpointConfigurator
{
    TimeSpan PollingInterval { set; }
    TimeSpan? AutoDeleteOnIdle { set; }
    TimeSpan? MessageTimeToLive { set; }
}