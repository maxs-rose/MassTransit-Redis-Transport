using MassTransit;

namespace RedisTransport.Transport.Configuration;

public interface IRedisReceiveEndpointConfigurator : IReceiveEndpointConfigurator
{
    TimeSpan? AutoDeleteOnIdle { set; }

    TimeSpan? MessageTimeToLive { set; }
}