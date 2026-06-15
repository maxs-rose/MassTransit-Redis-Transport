using MassTransit;
using MassTransit.Configuration;
using RedisTransport.Transport;
using StackExchange.Redis;

namespace RedisTransport.Configuration;

internal sealed class RedisBusFactoryConfigurator : BusFactoryConfigurator, IRedisBusFactoryConfigurator, IBusFactory
{
    private readonly IRedisBusConfiguration _busConfiguration;
    private readonly IRedisHostConfiguration _hostConfiguration;
    private readonly RedisReceiveSettings _settings;
    private Func<Task<IConnectionMultiplexer>>? _multiplexerFactory;

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

    public void Host(string connectionString)
    {
        Host(ConfigurationOptions.Parse(connectionString));
    }

    public void Host(ConfigurationOptions options)
    {
        _multiplexerFactory = async () => await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
    }

    public void Host(Action<ConfigurationOptions> configure)
    {
        var options = new ConfigurationOptions();
        configure(options);
        Host(options);
    }

    public void Host(Func<ConfigurationOptions, Task> configureAsync)
    {
        _multiplexerFactory = async () =>
        {
            var options = new ConfigurationOptions();
            await configureAsync(options).ConfigureAwait(false);
            return await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
        };
    }

    public void Host(string connectionString, Func<ConfigurationOptions, Task> configureAsync)
    {
        var options = ConfigurationOptions.Parse(connectionString);
        _multiplexerFactory = async () =>
        {
            await configureAsync(options).ConfigureAwait(false);
            return await ConnectionMultiplexer.ConnectAsync(options).ConfigureAwait(false);
        };
    }

    public void ReceiveEndpoint(IEndpointDefinition definition, IEndpointNameFormatter? endpointNameFormatter = null,
        Action<IReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        _hostConfiguration.ReceiveEndpoint(definition, endpointNameFormatter, configureEndpoint);
    }

    public void ReceiveEndpoint(string queueName, Action<IReceiveEndpointConfigurator> configureEndpoint)
    {
        _hostConfiguration.ReceiveEndpoint(queueName, configureEndpoint);
    }

    public void ReceiveEndpoint(IEndpointDefinition definition, IEndpointNameFormatter? endpointNameFormatter = null,
        Action<IRedisReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        _hostConfiguration.ReceiveEndpoint(definition, endpointNameFormatter, configureEndpoint);
    }

    public void ReceiveEndpoint(string queueName, Action<IRedisReceiveEndpointConfigurator> configureEndpoint)
    {
        _hostConfiguration.ReceiveEndpoint(queueName, configureEndpoint);
    }

    public void WithPrefix(string prefix)
    {
        RedisKeys.KeyPrefix = prefix;
    }

    internal Task<IConnectionMultiplexer> CreateMultiplexer()
    {
        return _multiplexerFactory!();
    }
}