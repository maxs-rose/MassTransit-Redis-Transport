using MassTransit;
using MassTransit.Transports;
using RedisTransport.Telemetry;
using RedisTransport.Transport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisMessageReceiver : ConsumerAgent<string>
{
    private readonly string _consumerGroup;
    private readonly string _consumerName;
    private readonly ReceiveEndpointContext _context;
    private readonly IRedisHostConfiguration _hostConfiguration;
    private readonly string _notifyChannel;
    private readonly RedisReceiveSettings _settings;
    private readonly string _streamKey;
    private TaskCompletionSource<bool> _wakeup = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public RedisMessageReceiver(
        ReceiveEndpointContext context,
        IRedisHostConfiguration hostConfiguration,
        RedisReceiveSettings settings,
        string streamKey,
        string notifyChannel,
        string consumerGroup) : base(context)
    {
        _context = context;
        _hostConfiguration = hostConfiguration;
        _settings = settings;
        _streamKey = streamKey;
        _notifyChannel = notifyChannel;
        _consumerGroup = consumerGroup;
        _consumerName = $"{Environment.MachineName}-{Guid.NewGuid():N}";

        TrySetConsumeTask(Task.Run(Consume));
    }

    private async Task Consume()
    {
        ChannelMessageQueue? notifyQueue = null;
        try
        {
            await EnsureConsumerGroup().ConfigureAwait(false);

            var subscriber = _hostConfiguration.Multiplexer.GetSubscriber();
            var channel = RedisChannel.Literal(_notifyChannel);

            try
            {
                notifyQueue = await subscriber.SubscribeAsync(channel).ConfigureAwait(false);
                notifyQueue.OnMessage(_ => Volatile.Read(ref _wakeup).TrySetResult(true));
            }
            catch (Exception ex)
            {
                LogContext.Warning?.Log(ex, "Failed to subscribe to {Channel}; falling back to polling", _notifyChannel);
            }

            SetReady();

            var db = _hostConfiguration.Multiplexer.GetDatabase();

            while (!IsStopping)
                try
                {
                    using var t = Otel.TraceActivitySource.StartActivity($"Poll: {_streamKey}");

                    await TrimExpiredMessages(db).ConfigureAwait(false);

                    var entries = await db.StreamReadGroupAsync(_streamKey, _consumerGroup, _consumerName, ">", _settings.PrefetchCount).ConfigureAwait(false);

                    if (entries is { Length: > 0 })
                    {
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
                    LogContext.Warning?.Log(ex, "Redis receive loop iteration faulted on {Stream}", _streamKey);
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
            LogContext.Error?.Log(ex, "Redis receive loop terminated for {Stream}", _streamKey);
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
            Interlocked.CompareExchange(ref _wakeup, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously), current);
    }

    private async Task Dispatch(IDatabase db, StreamEntry entry)
    {
        if (IsStopping)
            return;

        var message = RedisTransportMessage.FromStreamEntry(_streamKey, entry);
        var lockContext = new RedisReceiveLockContext(_context.InputAddress, message, db, _consumerGroup);
        using var receiveContext = new RedisReceiveContext(message, _context, lockContext);

        try
        {
            await Dispatch(entry.Id.ToString(), receiveContext, lockContext).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            receiveContext.LogTransportFaulted(ex);
        }
    }

    private async Task EnsureConsumerGroup()
    {
        var db = _hostConfiguration.Multiplexer.GetDatabase();
        try
        {
            await db.StreamCreateConsumerGroupAsync(_streamKey, _consumerGroup, "0-0").ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
        }
    }
}