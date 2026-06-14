using MassTransit;
using MassTransit.Middleware;
using MassTransit.Transports;
using RedisTransport.Transport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport;

public sealed class RedisReceiveTransport(IRedisHostConfiguration hostConfiguration, ReceiveEndpointContext context, RedisReceiveSettings settings)
    : Agent, IReceiveTransport
{
    private static readonly TimeSpan MaxRefreshInterval = TimeSpan.FromMinutes(1);
    private readonly CancellationTokenSource _lifecycleCts = new();

    private readonly List<string> _subscribedTopics = new();
    private RedisMessageReceiver? _receiver;

    public ReceiveTransportHandle Start()
    {
        if (context is QueueRedisReceiveEndpointContext queueContext)
            foreach (var type in queueContext.SubscribedMessageTypes)
                _subscribedTopics.Add(MessageTypeNameFormatter.Format(type));

        _ = RegisterAsync();

        if (settings.AutoDeleteOnIdle.HasValue)
            _ = RefreshLoop(_lifecycleCts.Token);

        _receiver = new RedisMessageReceiver(
            context,
            hostConfiguration,
            settings,
            RedisKeys.QueueStream(settings.QueueName),
            RedisKeys.QueueNotify(settings.QueueName),
            settings.QueueName);

        _ = NotifyReadyAsync(_receiver);

        return new Handle(this);
    }

    public void Probe(ProbeContext context1)
    {
        var scope = context1.CreateScope("receiveTransport");
        context.Probe(scope);
    }

    public ConnectHandle ConnectReceiveObserver(IReceiveObserver observer)
    {
        return context.ConnectReceiveObserver(observer);
    }

    public ConnectHandle ConnectReceiveTransportObserver(IReceiveTransportObserver observer)
    {
        return context.ConnectReceiveTransportObserver(observer);
    }

    public ConnectHandle ConnectPublishObserver(IPublishObserver observer)
    {
        return context.ConnectPublishObserver(observer);
    }

    public ConnectHandle ConnectSendObserver(ISendObserver observer)
    {
        return context.ConnectSendObserver(observer);
    }

    private async Task RegisterAsync()
    {
        try
        {
            var db = hostConfiguration.Multiplexer.GetDatabase();
            var queueName = settings.QueueName;

            if (settings.AutoDeleteOnIdle.HasValue)
                await EnsureStreamExistsAsync(db, queueName).ConfigureAwait(false);

            var pending = new List<Task>();
            foreach (var topic in _subscribedTopics)
                pending.Add(db.HashSetAsync(RedisKeys.TopicSubscribers(topic), queueName, "1", flags: CommandFlags.FireAndForget));

            if (pending.Count > 0)
                await Task.WhenAll(pending).ConfigureAwait(false);

            if (settings.AutoDeleteOnIdle.HasValue)
                await ApplyExpiriesAsync(db).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogContext.Warning?.Log(ex, "Failed to register subscriptions for {Queue}", settings.QueueName);
        }
    }

    private async Task EnsureStreamExistsAsync(IDatabase db, string queueName)
    {
        try
        {
            await db.StreamCreateConsumerGroupAsync(RedisKeys.QueueStream(queueName), queueName, "0-0", true).ConfigureAwait(false);
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

    private async Task RefreshLoop(CancellationToken ct)
    {
        var ttl = settings.AutoDeleteOnIdle!.Value;
        var interval = TimeSpan.FromTicks(ttl.Ticks / 2);
        if (interval > MaxRefreshInterval)
            interval = MaxRefreshInterval;
        if (interval < TimeSpan.FromSeconds(1))
            interval = TimeSpan.FromSeconds(1);

        while (!ct.IsCancellationRequested)
            try
            {
                await Task.Delay(interval, ct).ConfigureAwait(false);
                await ApplyExpiriesAsync(hostConfiguration.Multiplexer.GetDatabase()).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                LogContext.Debug?.Log(ex, "TTL refresh faulted for {Queue}", settings.QueueName);
            }
    }

    private async Task NotifyReadyAsync(RedisMessageReceiver receiver)
    {
        try
        {
            await receiver.Ready.ConfigureAwait(false);
            await context.TransportObservers.NotifyReady(context.InputAddress).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogContext.Warning?.Log(ex, "Receiver ready notification failed for {Address}", context.InputAddress);
        }
    }

    private async Task StopAndCleanupAsync(CancellationToken cancellationToken)
    {
        _lifecycleCts.Cancel();

        if (_receiver != null)
            try
            {
                await _receiver.Stop("Receive transport stopping", cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogContext.Debug?.Log(ex, "Receiver stop faulted for {Address}", context.InputAddress);
            }

        if (!settings.AutoDeleteOnIdle.HasValue)
            return;

        try
        {
            var db = hostConfiguration.Multiplexer.GetDatabase();
            var pending = new List<Task>();

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

    private sealed class Handle : Agent, ReceiveTransportHandle
    {
        private readonly RedisReceiveTransport _transport;

        public Handle(RedisReceiveTransport transport)
        {
            _transport = transport;
            SetReady();
        }

        public Task Stop(CancellationToken cancellationToken)
        {
            return this.Stop("Stop Receive Transport", cancellationToken);
        }

        protected override async Task StopAgent(StopContext context)
        {
            await _transport.StopAndCleanupAsync(context.CancellationToken).ConfigureAwait(false);
            await base.StopAgent(context).ConfigureAwait(false);
        }
    }
}