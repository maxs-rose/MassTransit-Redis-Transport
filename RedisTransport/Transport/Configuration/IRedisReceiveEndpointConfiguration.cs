using MassTransit.Configuration;
using IHost = MassTransit.Transports.IHost;

namespace RedisTransport.Transport.Configuration;

public interface IRedisReceiveEndpointConfiguration : IReceiveEndpointConfiguration, IRedisEndpointConfiguration
{
    void Build(IHost host);
}