using MassTransit.Configuration;

namespace RedisTransport.Transport.Configuration;

public interface IRedisEndpointConfiguration : IEndpointConfiguration
{
    new IRedisTopologyConfiguration Topology { get; }
}