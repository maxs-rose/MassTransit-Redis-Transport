using MassTransit;
using MassTransit.Configuration;
using MassTransit.Observables;

namespace RedisTransport.Transport.Configuration;

internal sealed class RedisBusConfiguration : RedisEndpointConfiguration, IRedisBusConfiguration
{
    private readonly BusObservable _busObservers;

    public RedisBusConfiguration(IRedisTopologyConfiguration topologyConfiguration) : base(topologyConfiguration)
    {
        HostConfiguration = new RedisHostConfiguration(this, topologyConfiguration);
        BusEndpointConfiguration = CreateEndpointConfiguration(true);
        _busObservers = new BusObservable();
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
}