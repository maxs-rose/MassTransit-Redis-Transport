using MassTransit;
using MassTransit.Transports;
using RedisTransport.Transport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisSendTransportContext(
    IRedisHostConfiguration hostConfiguration,
    ReceiveEndpointContext receiveEndpointContext,
    string entityName,
    RedisEndpointAddress.AddressType addressType
) : BaseSendTransportContext(hostConfiguration, receiveEndpointContext.Serialization), ISendTransport
{
    public override string EntityName { get; } = entityName;
    public override string ActivitySystem => "redis";

    public override async Task<SendContext<T>> CreateSendContext<T>(T message, IPipe<SendContext<T>> pipe, CancellationToken cancellationToken)
    {
        var sendContext = new RedisMessageSendContext<T>(message, cancellationToken);

        await pipe.Send(sendContext).ConfigureAwait(false);

        return sendContext;
    }

    public async Task Send<T>(T message, IPipe<SendContext<T>> pipe, CancellationToken cancellationToken) where T : class
    {
        var sendContext = (RedisMessageSendContext<T>)await CreateSendContext(message, pipe, cancellationToken).ConfigureAwait(false);

        try
        {
            if (SendObservers.Count > 0)
                await SendObservers.PreSend(sendContext).ConfigureAwait(false);

            var entries = sendContext.RedisMessage();

            var db = hostConfiguration.Multiplexer.GetDatabase();
            var subscriber = hostConfiguration.Multiplexer.GetSubscriber();

            if (addressType == RedisEndpointAddress.AddressType.Topic)
                await FanoutToSubscribers(db, subscriber, entries).ConfigureAwait(false);
            else
                await SendToQueue(db, subscriber, EntityName, entries).ConfigureAwait(false);

            sendContext.LogSent();

            if (SendObservers.Count > 0)
                await SendObservers.PostSend(sendContext).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            sendContext.LogFaulted(ex);

            if (SendObservers.Count > 0)
                await SendObservers.SendFault(sendContext, ex).ConfigureAwait(false);

            throw;
        }
    }

    private async Task FanoutToSubscribers(IDatabase db, ISubscriber subscriber, NameValueEntry[] entries)
    {
        var subscribers = await db.HashKeysAsync(RedisKeys.TopicSubscribers(EntityName)).ConfigureAwait(false);
        if (subscribers.Length == 0)
            return;

        var pending = new List<Task>(subscribers.Length * 2);
        foreach (var sub in subscribers)
        {
            var queueName = (string?)sub;
            if (string.IsNullOrEmpty(queueName))
                continue;

            pending.Add(db.StreamAddAsync(RedisKeys.QueueStream(queueName), entries, flags: CommandFlags.FireAndForget));
            pending.Add(subscriber.PublishAsync(RedisChannel.Literal(RedisKeys.QueueNotify(queueName)), RedisValue.EmptyString, CommandFlags.FireAndForget));
        }

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    private async Task SendToQueue(IDatabase db, ISubscriber subscriber, string queueName, NameValueEntry[] entries)
    {
        await db.StreamAddAsync(RedisKeys.QueueStream(queueName), entries).ConfigureAwait(false);
        await subscriber.PublishAsync(RedisChannel.Literal(RedisKeys.QueueNotify(queueName)), RedisValue.EmptyString).ConfigureAwait(false);
    }
}