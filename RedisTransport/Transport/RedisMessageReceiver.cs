using System.Diagnostics;
using MassTransit;
using MassTransit.Transports;
using RedisTransport.Configuration;
using RedisTransport.Telemetry;
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
    private TaskCompletionSource<bool> _wakeup = new(TaskCreationOptions.RunContinuationsAsynchronously);

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

        if (settings.AutoDeleteOnIdle.HasValue)
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
                notifyQueue.OnMessage(_ => Volatile.Read(ref _wakeup).TrySetResult(true));
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

            while (!IsStopping)
                try
                {
                    using var t = Otel.TraceActivitySource.StartActivity(ActivityKind.Consumer);
                    t?.SetTag("messaging.system", "redis");
                    t?.SetTag("messaging.destination", _streamKey);

                    await TrimExpiredMessages(db).ConfigureAwait(false);

                    var entries = await db.StreamReadGroupAsync(
                            _streamKey, _consumerGroup, _consumerName, ">", _settings.PrefetchCount)
                        .ConfigureAwait(false);

                    if (entries is { Length: > 0 })
                    {
                        t?.SetTag("messaging.batch.message_count", entries.Length);
                        LogContext.Debug?.Log("Read {Count} message(s) from {Stream}", entries.Length, _streamKey);

                        foreach (var entry in entries)
                            await Dispatch(db, entry).ConfigureAwait(false);
                        continue;
                    }

                    await WaitForNotificationOrTimeout().ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (IsStopping)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogContext.Warning?.Log(ex, "Receive loop faulted on {Stream}; waiting before retry", _streamKey);
                    try
                    {
                        await Task.Delay(_settings.PollingInterval, Stopping).ConfigureAwait(false);
                    }
                    catch
                    {
                    }
                }
        }
        catch (Exception ex)
        {
            LogContext.Error?.Log(ex, "Receive loop terminated for {Stream}", _streamKey);
        }
        finally
        {
            if (notifyQueue != null)
                try
                {
                    await notifyQueue.UnsubscribeAsync().ConfigureAwait(false);
                }
                catch
                {
                }
        }
    }

    private async Task RefreshLoop()
    {
        var ttl = _settings.AutoDeleteOnIdle!.Value;
        var interval = TimeSpan.FromTicks(ttl.Ticks / 2);
        if (interval > MaxRefreshInterval) interval = MaxRefreshInterval;
        if (interval < TimeSpan.FromSeconds(1)) interval = TimeSpan.FromSeconds(1);

        while (!Stopping.IsCancellationRequested)
            try
            {
                await Task.Delay(interval, Stopping).ConfigureAwait(false);
                await ApplyExpiriesAsync(_clientContext.Database).ConfigureAwait(false);
                LogContext.Debug?.Log("Refreshed TTL for ephemeral queue {Queue}", _consumerGroup);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                LogContext.Debug?.Log(ex, "TTL refresh faulted for {Queue}", _consumerGroup);
            }
    }

    private async Task ApplyExpiriesAsync(IDatabase db)
    {
        var ttl = _settings.AutoDeleteOnIdle!.Value;
        var fields = new RedisValue[] { _consumerGroup };

        var pending = new List<Task>(_subscribedTopics.Count + 1);
        foreach (var topic in _subscribedTopics)
            pending.Add(db.HashFieldExpireAsync(RedisKeys.TopicSubscribers(topic), fields, ttl));
        pending.Add(db.KeyExpireAsync(_streamKey, ttl, CommandFlags.FireAndForget));

        await Task.WhenAll(pending).ConfigureAwait(false);
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

    private async Task WaitForNotificationOrTimeout()
    {
        var current = Volatile.Read(ref _wakeup);
        var delay = Task.Delay(_settings.PollingInterval, Stopping);

        try
        {
            await Task.WhenAny(current.Task, delay).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        if (current.Task.IsCompleted)
            Interlocked.CompareExchange(ref _wakeup,
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously), current);
    }

    private async Task Dispatch(IDatabase db, StreamEntry entry)
    {
        if (IsStopping)
            return;

        var message = RedisTransportMessage.FromStreamEntry(_streamKey, entry);

        using var activity = Otel.ActivitySource.StartActivity(ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "redis");
        activity?.SetTag("messaging.destination", _streamKey);
        activity?.SetTag("messaging.message_id", message.MessageId?.ToString());
        activity?.SetTag("messaging.message_type", message.MessageType);
        activity?.SetTag("messaging.redis.entry_id", entry.Id.ToString());

        LogContext.Debug?.Log("Dispatching message {MessageId} ({MessageType}) from {Stream}",
            message.MessageId, message.MessageType, _streamKey);

        var lockContext = new RedisReceiveLockContext(_context.InputAddress, message, db, _consumerGroup);
        using var receiveContext = new RedisReceiveContext(message, _context, lockContext);

        try
        {
            await Dispatch(entry.Id.ToString(), receiveContext, lockContext).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            receiveContext.LogTransportFaulted(ex);
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
