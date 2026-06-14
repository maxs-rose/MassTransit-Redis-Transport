using MassTransit;
using MassTransit.Configuration;
using MassTransit.Observables;
using MassTransit.Topology;

namespace RedisTransport.Configuration;

internal sealed class RedisBusConfiguration : RedisEndpointConfiguration, IRedisBusConfiguration
{
    private readonly BusObservable _busObservers = new();

    public RedisBusConfiguration(IRedisTopologyConfiguration topologyConfiguration) : base(topologyConfiguration)
    {
        HostConfiguration = new RedisHostConfiguration(this, topologyConfiguration);
        BusEndpointConfiguration = CreateEndpointConfiguration(true);
    }

    IHostConfiguration IBusConfiguration.HostConfiguration => HostConfiguration;
    IEndpointConfiguration IBusConfiguration.BusEndpointConfiguration => BusEndpointConfiguration;
    IBusObserver IBusConfiguration.BusObservers => _busObservers;

    public IRedisEndpointConfiguration BusEndpointConfiguration { get; }
    public IRedisHostConfiguration HostConfiguration { get; }

    public ConnectHandle ConnectBusObserver(IBusObserver observer)
    {
        return _busObservers.Connect(observer);
    }

    public ConnectHandle ConnectEndpointConfigurationObserver(IEndpointConfigurationObserver observer)
    {
        return HostConfiguration.ConnectEndpointConfigurationObserver(observer);
    }

    public static RedisBusConfiguration Create()
    {
        var messageTopology = new MessageTopology(Defaults.EntityNameFormatter);
        return new RedisBusConfiguration(new RedisTopologyConfiguration(messageTopology));
    }
}