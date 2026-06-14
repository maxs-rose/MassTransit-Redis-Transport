using MassTransit;
using MassTransit.Transports;
using RedisTransport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport.Middleware;

internal sealed class RedisConsumerFilter(QueueRedisReceiveEndpointContext context, RedisReceiveSettings settings)
    : IFilter<RedisClientContext>
{
    private static readonly TimeSpan MaxRefreshInterval = TimeSpan.FromMinutes(1);

    private readonly List<string> _subscribedTopics = context.SubscribedMessageTypes
        .Select(RedisMessageTypeFormatter.Format)
        .ToList();

    void IProbeSite.Probe(ProbeContext context) { }

    async Task IFilter<RedisClientContext>.Send(RedisClientContext clientContext, IPipe<RedisClientContext> next)
    {
        LogContext.Debug?.Log("Starting consumer for {QueueName} with {TopicCount} topic subscription(s)",
            settings.QueueName, _subscribedTopics.Count);

        await RegisterSubscriptionsAsync(clientContext.Database).ConfigureAwait(false);

        using var refreshCts = CancellationTokenSource.CreateLinkedTokenSource(clientContext.CancellationToken);
        if (settings.AutoDeleteOnIdle.HasValue)
            _ = RefreshLoop(clientContext.Database, refreshCts.Token);

        var receiver = new RedisMessageReceiver(
            clientContext,
            context,
            settings,
            RedisKeys.QueueStream(settings.QueueName),
            RedisKeys.QueueNotify(settings.QueueName),
            settings.QueueName);

        await receiver.Ready.ConfigureAwait(false);

        context.AddConsumeAgent(receiver);
        await context.TransportObservers.NotifyReady(context.InputAddress).ConfigureAwait(false);

        try
        {
            await receiver.Completed.ConfigureAwait(false);
        }
        finally
        {
            await refreshCts.CancelAsync().ConfigureAwait(false);

            DeliveryMetrics metrics = receiver;
            await context.TransportObservers.NotifyCompleted(context.InputAddress, metrics).ConfigureAwait(false);
            context.LogConsumerCompleted(metrics.DeliveryCount, metrics.ConcurrentDeliveryCount);

            if (settings.AutoDeleteOnIdle.HasValue)
                await CleanupAsync(clientContext.Database).ConfigureAwait(false);
        }
    }

    private async Task RegisterSubscriptionsAsync(IDatabase db)
    {
        try
        {
            if (settings.AutoDeleteOnIdle.HasValue)
                await EnsureStreamExistsAsync(db).ConfigureAwait(false);

            if (_subscribedTopics.Count > 0)
            {
                LogContext.Debug?.Log("Subscribing {QueueName} to {TopicCount} topic(s): {Topics}",
                    settings.QueueName, _subscribedTopics.Count, string.Join(", ", _subscribedTopics));

                var pending = _subscribedTopics.Select(topic =>
                    db.HashSetAsync(RedisKeys.TopicSubscribers(topic), settings.QueueName, "1", flags: CommandFlags.FireAndForget));

                await Task.WhenAll(pending).ConfigureAwait(false);
            }

            if (settings.AutoDeleteOnIdle.HasValue)
                await ApplyExpiriesAsync(db).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogContext.Warning?.Log(ex, "Failed to register subscriptions for {Queue}", settings.QueueName);
        }
    }

    private async Task EnsureStreamExistsAsync(IDatabase db)
    {
        try
        {
            await db.StreamCreateConsumerGroupAsync(
                RedisKeys.QueueStream(settings.QueueName), settings.QueueName, "0-0").ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
        }
    }

    private async Task ApplyExpiriesAsync(IDatabase db)
    {
        var ttl = settings.AutoDeleteOnIdle!.Value;
        var queueName = settings.QueueName;
        var fields = new RedisValue[] { queueName };

        var pending = new List<Task>(_subscribedTopics.Count + 1);
        foreach (var topic in _subscribedTopics)
            pending.Add(db.HashFieldExpireAsync(RedisKeys.TopicSubscribers(topic), fields, ttl));
        pending.Add(db.KeyExpireAsync(RedisKeys.QueueStream(queueName), ttl, CommandFlags.FireAndForget));

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    private async Task RefreshLoop(IDatabase db, CancellationToken ct)
    {
        var ttl = settings.AutoDeleteOnIdle!.Value;
        var interval = TimeSpan.FromTicks(ttl.Ticks / 2);
        if (interval > MaxRefreshInterval) interval = MaxRefreshInterval;
        if (interval < TimeSpan.FromSeconds(1)) interval = TimeSpan.FromSeconds(1);

        while (!ct.IsCancellationRequested)
            try
            {
                await Task.Delay(interval, ct).ConfigureAwait(false);
                await ApplyExpiriesAsync(db).ConfigureAwait(false);
                LogContext.Debug?.Log("Refreshed TTL for ephemeral queue {Queue}", settings.QueueName);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                LogContext.Debug?.Log(ex, "TTL refresh faulted for {Queue}", settings.QueueName);
            }
    }

    private async Task CleanupAsync(IDatabase db)
    {
        LogContext.Debug?.Log("Removing ephemeral queue {Queue} and {TopicCount} subscription(s)",
            settings.QueueName, _subscribedTopics.Count);

        try
        {
            var pending = new List<Task>(_subscribedTopics.Count + 1);
            foreach (var topic in _subscribedTopics)
                pending.Add(db.HashDeleteAsync(RedisKeys.TopicSubscribers(topic), settings.QueueName, CommandFlags.FireAndForget));
            pending.Add(db.KeyDeleteAsync(RedisKeys.QueueStream(settings.QueueName), CommandFlags.FireAndForget));
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogContext.Debug?.Log(ex, "Ephemeral cleanup faulted for {Queue}", settings.QueueName);
        }
    }
}
