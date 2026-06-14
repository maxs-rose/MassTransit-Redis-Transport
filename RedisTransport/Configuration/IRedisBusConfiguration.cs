using MassTransit.Configuration;

namespace RedisTransport.Configuration;

internal interface IRedisBusConfiguration : IBusConfiguration
{
    new IRedisHostConfiguration HostConfiguration { get; }
    new IRedisEndpointConfiguration BusEndpointConfiguration { get; }
    new IRedisTopologyConfiguration Topology { get; }

    IRedisEndpointConfiguration CreateEndpointConfiguration(bool isBusEndpoint = false);
}