using MassTransit;
using MassTransit.Transports;

namespace RedisTransport.Transport;

internal sealed class RedisReceiveContext : BaseReceiveContext
{
    public RedisReceiveContext(RedisTransportMessage message, ReceiveEndpointContext context, RedisReceiveLockContext lockContext)
        : base(message.DeliveryCount > 0, context, lockContext, message)
    {
        TransportMessage = message;
        Body = new StringMessageBody(message.Body ?? string.Empty);
    }

    public RedisTransportMessage TransportMessage { get; }

    public override MessageBody Body { get; }

    protected override IHeaderProvider HeaderProvider => field ??= new RedisHeaderProvider(TransportMessage);
}