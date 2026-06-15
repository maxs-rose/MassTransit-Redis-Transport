using MassTransit;
using MassTransit.Transports;
using MassTransit.Util;
using RedisTransport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisMessageReceiver : ConsumerAgent<string>
{
    private static readonly TimeSpan MaxRefreshInterval = TimeSpan.FromMinutes(1);

    private readonly RedisClientContext _clientContext;
    private readonly string _consumerGroup;
    private readonly string _consumerName;
    private readonly ReceiveEndpointContext _context;
    private readonly string _notifyChannel;
    private readonly RedisReceiveSettings _settings;
    private readonly string _streamKey;
    private readonly IReadOnlyList<string> _subscribedTopics;
    private CancellationTokenSource _notifyCts = new();

    public RedisMessageReceiver(
        RedisClientContext clientContext,
        ReceiveEndpointContext context,
        RedisReceiveSettings settings,
        string streamKey,
        string notifyChannel,
        string consumerGroup,
        IReadOnlyList<string> subscribedTopics) : base(context)
    {
        _clientContext = clientContext;
        _context = context;
        _settings = settings;
        _streamKey = streamKey;
        _notifyChannel = notifyChannel;
        _consumerGroup = consumerGroup;
        _consumerName = $"{Environment.MachineName}-{Guid.NewGuid():N}";
        _subscribedTopics = subscribedTopics;

        TrySetConsumeTask(Task.Run(Consume));

        if (settings.AutoDeleteOnIdle.HasValue || subscribedTopics.Count > 0)
            _ = Task.Run(RefreshLoop);
    }

    private async Task Consume()
    {
        ChannelMessageQueue? notifyQueue = null;
        try
        {
            await EnsureConsumerGroup().ConfigureAwait(false);

            try
            {
                var channel = RedisChannel.Literal(_notifyChannel);
                notifyQueue = await _clientContext.Subscriber.SubscribeAsync(channel).ConfigureAwait(false);
                notifyQueue.OnMessage(_ => Interlocked.Exchange(ref _notifyCts, new CancellationTokenSource()).Cancel());
                LogContext.Debug?.Log("Subscribed to notify channel {Channel}", _notifyChannel);
            }
            catch (Exception ex)
            {
                LogContext.Warning?.Log(ex, "Failed to subscribe to notify channel {Channel}; falling back to polling", _notifyChannel);
            }

            LogContext.Debug?.Log("Receiver ready on stream {Stream} (consumer: {ConsumerName}, group: {ConsumerGroup})",
                _streamKey, _consumerName, _consumerGroup);
            SetReady();

            var db = _clientContext.Database;

            using var algorithm = new RequestRateAlgorithm(new RequestRateAlgorithmOptions
            {
                PrefetchCount = _settings.PrefetchCount,
                RequestResultLimit = _settings.PrefetchCount
            });

            while (!IsStopping)
                await algorithm.Run(async (limit, ct) =>
                    {
                        try
                        {
                            return await ReceiveMessages(db, limit, ct);
                        }
                        // Someone has deleted the queue manually in redis so just recreate it
                        catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP", StringComparison.OrdinalIgnoreCase))
                        {
                            await EnsureConsumerGroup().ConfigureAwait(false);
                            return [];
                        }
                    },
                    (entry, _) => HandleMessage(db, entry),
                    Stopping).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (IsStopping)
        {
        }
        catch (Exception ex)
        {
            LogContext.Error?.Log(ex, "Receive loop terminated for {Stream}", _streamKey);
        }
        finally
        {
            if (notifyQueue != null)
                await notifyQueue.UnsubscribeAsync().ConfigureAwait(false);
        }
    }

    private async Task<IEnumerable<StreamEntry>> ReceiveMessages(IDatabase db, int limit, CancellationToken ct)
    {
        await TrimExpiredMessages(db).ConfigureAwait(false);

        var entries = await db.StreamReadGroupAsync(
                _streamKey, _consumerGroup, _consumerName, ">", limit)
            .ConfigureAwait(false);

        if (entries is { Length: 0 })
        {
            await WaitForNotificationOrTimeout(ct).ConfigureAwait(false);
            return [];
        }

        LogContext.Debug?.Log("Read {Count} message(s) from {Stream}", entries.Length, _streamKey);

        return entries;
    }

    private async Task HandleMessage(IDatabase db, StreamEntry entry)
    {
        if (IsStopping)
            return;

        var message = RedisTransportMessage.FromStreamEntry(_streamKey, entry);

        var lockContext = new RedisReceiveLockContext(_context.InputAddress, message, db, _consumerGroup);

        if (message.ExpirationTime.HasValue && message.ExpirationTime.Value < DateTimeOffset.UtcNow)
        {
            await lockContext.Complete().ConfigureAwait(false);
            return;
        }

        LogContext.Debug?.Log("Dispatching message {MessageId} ({MessageType}) from {Stream}",
            message.MessageId, message.MessageType, _streamKey);

        using var receiveContext = new RedisReceiveContext(message, _context, lockContext);

        try
        {
            await Dispatch(entry.Id.ToString(), receiveContext, lockContext).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            receiveContext.LogTransportFaulted(ex);
        }
        finally
        {
            MessageHandled();
        }
    }

    private void MessageHandled()
    {
        Interlocked.Exchange(ref _notifyCts, new CancellationTokenSource()).Cancel();
    }

    private async Task WaitForNotificationOrTimeout(CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _notifyCts.Token);
        try
        {
            await Task.Delay(_settings.PollingInterval, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task RefreshLoop()
    {
        var interval = GetRefreshInterval();

        while (!Stopping.IsCancellationRequested)
            try
            {
                await Task.Delay(interval, Stopping).ConfigureAwait(false);
                await EnsureTopics(_clientContext.Database).ConfigureAwait(false);
                await ApplyExpiriesAsync(_clientContext.Database).ConfigureAwait(false);
                LogContext.Debug?.Log("Refreshed subscriptions/TTL for queue {Queue}", _consumerGroup);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                LogContext.Debug?.Log(ex, "Subscription refresh faulted for {Queue}", _consumerGroup);
            }
    }

    private TimeSpan GetRefreshInterval()
    {
        if (!_settings.AutoDeleteOnIdle.HasValue)
            return MaxRefreshInterval;

        var ttl = _settings.AutoDeleteOnIdle.Value;
        var interval = TimeSpan.FromTicks(ttl.Ticks / 2);

        if (interval > MaxRefreshInterval) interval = MaxRefreshInterval;
        if (interval < TimeSpan.FromSeconds(1)) interval = TimeSpan.FromSeconds(1);

        return interval;
    }

    private async Task ApplyExpiriesAsync(IDatabase db)
    {
        if (!_settings.AutoDeleteOnIdle.HasValue)
            return;

        var ttl = _settings.AutoDeleteOnIdle.Value;
        var fields = new RedisValue[] { _consumerGroup };

        var pending = new List<Task>(_subscribedTopics.Count + 1);
        foreach (var topic in _subscribedTopics)
            pending.Add(db.HashFieldExpireAsync(RedisKeys.TopicSubscribers(topic), fields, ttl));
        pending.Add(db.KeyExpireAsync(_streamKey, ttl, CommandFlags.FireAndForget));

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    private async Task EnsureTopics(IDatabase db)
    {
        foreach (var topic in _subscribedTopics)
            if (!await db.HashExistsAsync(RedisKeys.TopicSubscribers(topic), _consumerGroup).ConfigureAwait(false))
            {
                LogContext.Warning?.Log("Subscription for {Topic} on queue {Queue} was deleted; re-registering", topic, _consumerGroup);
                await db.HashSetAsync(RedisKeys.TopicSubscribers(topic), _consumerGroup, "1").ConfigureAwait(false);
            }
    }

    private async Task TrimExpiredMessages(IDatabase db)
    {
        if (!_settings.MessageTimeToLive.HasValue)
            return;

        var cutoffMs = DateTimeOffset.UtcNow.Subtract(_settings.MessageTimeToLive.Value).ToUnixTimeMilliseconds();
        if (cutoffMs <= 0)
            return;

        try
        {
            await db.ExecuteAsync("XTRIM", _streamKey, "MINID", "~", $"{cutoffMs}-0").ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogContext.Debug?.Log(ex, "XTRIM MINID faulted on {Stream}", _streamKey);
        }
    }

    private async Task EnsureConsumerGroup()
    {
        var db = _clientContext.Database;
        try
        {
            await db.StreamCreateConsumerGroupAsync(_streamKey, _consumerGroup, "0-0").ConfigureAwait(false);
            LogContext.Debug?.Log("Created consumer group {ConsumerGroup} on {Stream}", _consumerGroup, _streamKey);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            LogContext.Debug?.Log("Consumer group {ConsumerGroup} already exists on {Stream}", _consumerGroup, _streamKey);
        }
    }
}