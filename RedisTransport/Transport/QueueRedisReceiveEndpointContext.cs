using MassTransit;
using MassTransit.Configuration;
using MassTransit.Transports;
using RedisTransport.Transport.Configuration;

namespace RedisTransport.Transport;

public sealed class QueueRedisReceiveEndpointContext(IRedisHostConfiguration hostConfiguration, IReceiveEndpointConfiguration configuration, IReadOnlyCollection<Type> subscribedMessageTypes)
    : BaseReceiveEndpointContext(hostConfiguration, configuration)
{
    public IReadOnlyCollection<Type> SubscribedMessageTypes { get; } = subscribedMessageTypes;

    public override void AddSendAgent(IAgent agent)
    {
    }

    public override void AddConsumeAgent(IAgent agent)
    {
    }

    public override Exception ConvertException(Exception exception, string message)
    {
        return new ConnectionException(message + hostConfiguration.HostAddress, exception);
    }

    protected override ISendTransportProvider CreateSendTransportProvider()
    {
        return new RedisSendTransportProvider(hostConfiguration, this);
    }

    protected override IPublishTransportProvider CreatePublishTransportProvider()
    {
        return new RedisPublishTransportProvider(hostConfiguration, this);
    }
}