using MassTransit.Configuration;

namespace RedisTransport.Transport.Configuration;

internal class RedisEndpointConfiguration : EndpointConfiguration, IRedisEndpointConfiguration
{
    protected RedisEndpointConfiguration(IRedisTopologyConfiguration topologyConfiguration) : base(topologyConfiguration)
    {
        Topology = topologyConfiguration;
    }

    private RedisEndpointConfiguration(
        IEndpointConfiguration configuration,
        IRedisTopologyConfiguration topologyConfiguration,
        bool isBusEndpoint)
        : base(configuration, topologyConfiguration, isBusEndpoint)
    {
        Topology = topologyConfiguration;
    }

    public new IRedisTopologyConfiguration Topology { get; }

    public IRedisEndpointConfiguration CreateEndpointConfiguration(bool isBusEndpoint)
    {
        var topologyConfiguration = new RedisTopologyConfiguration(Topology);

        return new RedisEndpointConfiguration(this, topologyConfiguration, isBusEndpoint);
    }
}