using MassTransit;
using MassTransit.Configuration;
using MassTransit.Transports;
using RedisTransport.Configuration;
using RedisTransport.Transport.Middleware;

namespace RedisTransport.Transport;

internal sealed class QueueRedisReceiveEndpointContext : BaseReceiveEndpointContext
{
    private readonly IRedisHostConfiguration _hostConfiguration;
    private readonly object _supervisorLock = new();
    private IRedisClientContextSupervisor? _supervisor;

    public QueueRedisReceiveEndpointContext(
        IRedisHostConfiguration hostConfiguration,
        IReceiveEndpointConfiguration configuration,
        IReadOnlyCollection<Type> subscribedMessageTypes)
        : base(hostConfiguration, configuration)
    {
        _hostConfiguration = hostConfiguration;
        SubscribedMessageTypes = subscribedMessageTypes;
    }

    public IReadOnlyCollection<Type> SubscribedMessageTypes { get; }

    public IRedisClientContextSupervisor GetOrCreateSupervisor()
    {
        lock (_supervisorLock)
        {
            if (_supervisor is null || _supervisor.Completed.IsCompleted)
                _supervisor = new RedisClientContextSupervisor(_hostConfiguration.Multiplexer);
            return _supervisor;
        }
    }

    public override void AddSendAgent(IAgent agent)
    {
        _supervisor?.AddSendAgent(agent);
    }

    public override void AddConsumeAgent(IAgent agent)
    {
        _supervisor?.AddConsumeAgent(agent);
    }

    public override Exception ConvertException(Exception exception, string message)
    {
        return new ConnectionException(message + _hostConfiguration.HostAddress, exception);
    }

    protected override ISendTransportProvider CreateSendTransportProvider()
    {
        return new RedisSendTransportProvider(_hostConfiguration, this);
    }

    protected override IPublishTransportProvider CreatePublishTransportProvider()
    {
        return new RedisPublishTransportProvider(_hostConfiguration, this);
    }
}