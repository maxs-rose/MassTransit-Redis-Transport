using MassTransit.Configuration;

namespace RedisTransport.Configuration;

internal interface IRedisEndpointConfiguration : IEndpointConfiguration
{
    new IRedisTopologyConfiguration Topology { get; }
}