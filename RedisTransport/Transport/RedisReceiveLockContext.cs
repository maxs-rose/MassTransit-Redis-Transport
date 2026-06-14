using MassTransit;
using MassTransit.Transports;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisReceiveLockContext(Uri inputAddress, RedisTransportMessage message, IDatabase database, string consumerGroup)
    : ReceiveLockContext
{
    private bool _locked = true;

    public async Task Complete()
    {
        if (!_locked)
            return;

        try
        {
            await database.StreamAcknowledgeAsync(message.StreamKey, consumerGroup, message.EntryId).ConfigureAwait(false);
            await database.StreamDeleteAsync(message.StreamKey, [message.EntryId]).ConfigureAwait(false);
            _locked = false;
        }
        catch (Exception ex)
        {
            LogContext.Warning?.Log(ex, "Failed to ack/delete stream entry {EntryId} on {Stream}", message.EntryId, message.StreamKey);
        }
    }

    public Task Faulted(Exception exception)
    {
        // Leave entry in the PEL so XAUTOCLAIM redelivers it after LockDuration idle time.
        return Task.CompletedTask;
    }

    public Task ValidateLockStatus()
    {
        if (_locked)
            return Task.CompletedTask;

        throw new TransportException(inputAddress, $"Stream entry lock lost: {message.EntryId}");
    }
}