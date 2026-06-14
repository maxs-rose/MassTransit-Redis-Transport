using MassTransit;
using MassTransit.Configuration;
using MassTransit.Topology;
using MassTransit.Transports;
using RedisTransport.Transport.Configuration;
using IHost = MassTransit.Transports.IHost;

namespace RedisTransport.Transport;

public sealed class RedisReceiveEndpointConfiguration :
    ReceiveEndpointConfiguration,
    IRedisReceiveEndpointConfiguration,
    IRedisReceiveEndpointConfigurator
{
    readonly IRedisHostConfiguration _hostConfiguration;
    readonly IRedisEndpointConfiguration _endpointConfiguration;
    readonly RedisReceiveSettings _settings;
    readonly Lazy<Uri> _inputAddress;

    public RedisReceiveEndpointConfiguration(IRedisHostConfiguration hostConfiguration, RedisReceiveSettings settings, IRedisEndpointConfiguration endpointConfiguration)
        : base(hostConfiguration, endpointConfiguration)
    {
        _hostConfiguration = hostConfiguration;
        _settings = settings;
        _endpointConfiguration = endpointConfiguration;

        _inputAddress = new Lazy<Uri>(() => new RedisEndpointAddress(hostConfiguration.HostAddress, _settings.QueueName));
    }

    public RedisReceiveSettings Settings => _settings;

    public TimeSpan? AutoDeleteOnIdle
    {
        set => _settings.AutoDeleteOnIdle = value;
    }

    public TimeSpan? MessageTimeToLive
    {
        set => _settings.MessageTimeToLive = value;
    }

    public override Uri HostAddress => _hostConfiguration.HostAddress;
    public override Uri InputAddress => _inputAddress.Value;

    public new IRedisTopologyConfiguration Topology => _endpointConfiguration.Topology;

    public override ReceiveEndpointContext CreateReceiveEndpointContext()
    {
        return CreateRedisReceiveEndpointContext();
    }

    public void Build(IHost host)
    {
        var context = CreateRedisReceiveEndpointContext();

        var transport = new RedisReceiveTransport(_hostConfiguration, context, _settings);

        var receiveEndpoint = new ReceiveEndpoint(transport, context);

        host.AddReceiveEndpoint(_settings.QueueName, receiveEndpoint);

        ReceiveEndpoint = receiveEndpoint;
    }

    QueueRedisReceiveEndpointContext CreateRedisReceiveEndpointContext()
    {
        var builder = new RedisReceiveEndpointBuilder(this);
        ApplySpecifications(builder);
        var context = new QueueRedisReceiveEndpointContext(_hostConfiguration, this, builder.SubscribedMessageTypes);

        var errorQueueName = DefaultErrorQueueNameFormatter.Instance.FormatErrorQueueName(_settings.QueueName);
        context.GetOrAddPayload<IErrorTransport>(() => new RedisErrorTransport(errorQueueName, _hostConfiguration));

        return context;
    }
}
