using MassTransit;
using MassTransit.Configuration;

namespace RedisTransport.Transport.Configuration;

internal sealed class RedisBusFactoryConfigurator : BusFactoryConfigurator, IRedisBusFactoryConfigurator, IBusFactory
{
    private readonly IRedisBusConfiguration _busConfiguration;
    private readonly IRedisHostConfiguration _hostConfiguration;
    private readonly RedisReceiveSettings _settings;

    public RedisBusFactoryConfigurator(IRedisBusConfiguration busConfiguration) : base(busConfiguration)
    {
        _busConfiguration = busConfiguration;
        _hostConfiguration = busConfiguration.HostConfiguration;

        var queueName = busConfiguration.Topology.Consume.CreateTemporaryQueueName("bus");
        _settings = new RedisReceiveSettings(queueName, Defaults.TemporaryAutoDeleteOnIdle);
    }

    public IReceiveEndpointConfiguration CreateBusEndpointConfiguration(Action<IReceiveEndpointConfigurator> configure)
    {
        return _busConfiguration.HostConfiguration.CreateReceiveEndpointConfiguration(_settings, _busConfiguration.BusEndpointConfiguration, c => configure(c));
    }

    public void ReceiveEndpoint(IEndpointDefinition definition, IEndpointNameFormatter? endpointNameFormatter = null, Action<IReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        _hostConfiguration.ReceiveEndpoint(definition, endpointNameFormatter, configureEndpoint);
    }

    public void ReceiveEndpoint(string queueName, Action<IReceiveEndpointConfigurator> configureEndpoint)
    {
        _hostConfiguration.ReceiveEndpoint(queueName, configureEndpoint);
    }

    public void ReceiveEndpoint(IEndpointDefinition definition, IEndpointNameFormatter? endpointNameFormatter = null, Action<IRedisReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        _hostConfiguration.ReceiveEndpoint(definition, endpointNameFormatter, configureEndpoint);
    }

    public void ReceiveEndpoint(string queueName, Action<IRedisReceiveEndpointConfigurator> configureEndpoint)
    {
        _hostConfiguration.ReceiveEndpoint(queueName, configureEndpoint);
    }
}