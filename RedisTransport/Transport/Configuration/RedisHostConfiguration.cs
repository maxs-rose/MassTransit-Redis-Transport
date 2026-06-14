using MassTransit;
using MassTransit.Configuration;
using StackExchange.Redis;
using IHost = MassTransit.Transports.IHost;

namespace RedisTransport.Transport.Configuration;

public sealed class RedisHostConfiguration : BaseHostConfiguration<IRedisReceiveEndpointConfiguration, IRedisReceiveEndpointConfigurator>, IRedisHostConfiguration
{
    private readonly IRedisBusConfiguration _busConfiguration;

    public RedisHostConfiguration(IRedisBusConfiguration busConfiguration, IRedisTopologyConfiguration topologyConfiguration) : base(busConfiguration)
    {
        _busConfiguration = busConfiguration;
        Topology = new BusTopology(this, topologyConfiguration);

        ReceiveTransportRetryPolicy = Retry.CreatePolicy(x =>
        {
            x.Handle<ConnectionException>();
            x.Handle<RedisConnectionException>();
            x.Handle<RedisTimeoutException>();
            x.Exponential(1000, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3));
        });

        HostAddress = new Uri("redis://localhost/");
    }

    public IConnectionMultiplexer Multiplexer { get; set; } = null!;

    public override Uri HostAddress { get; }
    public override IBusTopology Topology { get; }

    public override IRetryPolicy ReceiveTransportRetryPolicy { get; }

    public IRedisReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(string queueName, Action<IRedisReceiveEndpointConfigurator>? configure = null)
    {
        var endpointConfiguration = (IRedisEndpointConfiguration)_busConfiguration.CreateEndpointConfiguration();
        var settings = new RedisReceiveSettings(endpointConfiguration, queueName);

        return CreateReceiveEndpointConfiguration(settings, endpointConfiguration, configure);
    }

    public IRedisReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(RedisReceiveSettings settings, IRedisEndpointConfiguration endpointConfiguration,
        Action<IRedisReceiveEndpointConfigurator>? configure = null)
    {
        var configuration = new RedisReceiveEndpointConfiguration(this, settings, endpointConfiguration);

        configure?.Invoke(configuration);

        Observers.EndpointConfigured(configuration);

        Add(configuration);

        return configuration;
    }

    public override IReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(string queueName, Action<IReceiveEndpointConfigurator>? configure)
    {
        return CreateReceiveEndpointConfiguration(queueName, configure == null ? null : c => configure(c));
    }

    public override IHost Build()
    {
        var host = new RedisHost(this, Topology);

        foreach (var endpointConfiguration in GetConfiguredEndpoints())
            endpointConfiguration.Build(host);

        return host;
    }

    public override void ReceiveEndpoint(IEndpointDefinition definition, IEndpointNameFormatter? endpointNameFormatter, Action<IRedisReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        var queueName = definition.GetEndpointName(endpointNameFormatter ?? DefaultEndpointNameFormatter.Instance);
        ReceiveEndpoint(queueName, configurator =>
        {
            ApplyRedisEndpointDefinition(configurator, definition);
            configureEndpoint?.Invoke(configurator);
        });
    }

    public override void ReceiveEndpoint(string queueName, Action<IRedisReceiveEndpointConfigurator> configureEndpoint)
    {
        CreateReceiveEndpointConfiguration(queueName, configureEndpoint);
    }

    private static void ApplyRedisEndpointDefinition(IRedisReceiveEndpointConfigurator configurator, IEndpointDefinition definition)
    {
        if (definition.IsTemporary)
            configurator.AutoDeleteOnIdle = Defaults.TemporaryAutoDeleteOnIdle;

        configurator.ConfigureConsumeTopology = definition.ConfigureConsumeTopology;
        configurator.ConcurrentMessageLimit = definition.ConcurrentMessageLimit;

        if (definition.PrefetchCount.HasValue)
            configurator.PrefetchCount = (ushort)definition.PrefetchCount.Value;
        else if (definition.ConcurrentMessageLimit.HasValue)
            configurator.PrefetchCount = (ushort)(definition.ConcurrentMessageLimit.Value * 12 / 10);

        definition.Configure(configurator);
    }
}

internal sealed class BusTopology(IRedisHostConfiguration host, IRedisTopologyConfiguration topology) : IBusTopology
{
    public IConsumeTopology ConsumeTopology => topology.Consume;

    public IMessagePublishTopology<T> Publish<T>() where T : class
    {
        return topology.Publish.GetMessageTopology<T>();
    }

    public IMessageSendTopology<T> Send<T>() where T : class
    {
        return topology.Send.GetMessageTopology<T>();
    }

    public IMessageTopology<T> Message<T>() where T : class
    {
        return topology.Message.GetMessageTopology<T>();
    }

    public ISendTopology SendTopology => topology.Send;
    public IPublishTopology PublishTopology => topology.Publish;

    public bool TryGetPublishAddress<T>(out Uri publishAddress) where T : class
    {
        publishAddress = new Uri(host.HostAddress, MessageTypeNameFormatter.Format(typeof(T)));
        return true;
    }

    public bool TryGetPublishAddress(Type messageType, out Uri publishAddress)
    {
        publishAddress = new Uri(host.HostAddress, MessageTypeNameFormatter.Format(messageType));
        return true;
    }
}