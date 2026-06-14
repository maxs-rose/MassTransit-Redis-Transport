using MassTransit.Configuration;

namespace RedisTransport.Transport.Configuration;

internal interface IRedisEndpointConfiguration : IEndpointConfiguration
{
    new IRedisTopologyConfiguration Topology { get; }
}