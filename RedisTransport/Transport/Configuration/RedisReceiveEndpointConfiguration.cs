using MassTransit.Configuration;
using MassTransit.Topology;
using MassTransit.Transports;
using RedisTransport.Transport.Configuration;
using IHost = MassTransit.Transports.IHost;

namespace RedisTransport.Transport;

internal sealed class RedisReceiveEndpointConfiguration :
    ReceiveEndpointConfiguration,
    IRedisReceiveEndpointConfiguration,
    IRedisReceiveEndpointConfigurator
{
    private readonly IRedisEndpointConfiguration _endpointConfiguration;
    private readonly IRedisHostConfiguration _hostConfiguration;
    private readonly Lazy<Uri> _inputAddress;

    public RedisReceiveEndpointConfiguration(IRedisHostConfiguration hostConfiguration, RedisReceiveSettings settings, IRedisEndpointConfiguration endpointConfiguration)
        : base(hostConfiguration, endpointConfiguration)
    {
        _hostConfiguration = hostConfiguration;
        Settings = settings;
        _endpointConfiguration = endpointConfiguration;

        _inputAddress = new Lazy<Uri>(() => new RedisEndpointAddress(hostConfiguration.HostAddress, Settings.QueueName));
    }

    public RedisReceiveSettings Settings { get; }

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

        var transport = new RedisReceiveTransport(_hostConfiguration, context, Settings);

        var receiveEndpoint = new ReceiveEndpoint(transport, context);

        host.AddReceiveEndpoint(Settings.QueueName, receiveEndpoint);

        ReceiveEndpoint = receiveEndpoint;
    }

    public TimeSpan? AutoDeleteOnIdle
    {
        set => Settings.AutoDeleteOnIdle = value;
    }

    public TimeSpan? MessageTimeToLive
    {
        set => Settings.MessageTimeToLive = value;
    }

    public TimeSpan PollingInterval
    {
        set => Settings.PollingInterval = value;
    }

    private QueueRedisReceiveEndpointContext CreateRedisReceiveEndpointContext()
    {
        var builder = new RedisReceiveEndpointBuilder(this);
        ApplySpecifications(builder);
        var context = new QueueRedisReceiveEndpointContext(_hostConfiguration, this, builder.SubscribedMessageTypes);

        var errorQueueName = DefaultErrorQueueNameFormatter.Instance.FormatErrorQueueName(Settings.QueueName);
        context.GetOrAddPayload<IErrorTransport>(() => new RedisErrorTransport(errorQueueName, _hostConfiguration));

        return context;
    }
}