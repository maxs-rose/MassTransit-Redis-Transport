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

            LogContext.Debug?.Log("Acknowledged message {EntryId} on {Stream}", message.EntryId, message.StreamKey);
        }
        catch (Exception ex)
        {
            LogContext.Warning?.Log(ex, "Failed to ack/delete stream entry {EntryId} on {Stream}", message.EntryId, message.StreamKey);
        }
    }

    public Task Faulted(Exception exception)
    {
        // Entry stays in PEL; XAUTOCLAIM redelivers it after idle timeout.
        LogContext.Debug?.Log(exception, "Message {EntryId} on {Stream} left in PEL for redelivery", message.EntryId, message.StreamKey);
        return Task.CompletedTask;
    }

    public Task ValidateLockStatus()
    {
        if (_locked)
            return Task.CompletedTask;

        throw new TransportException(inputAddress, $"Stream entry lock lost: {message.EntryId}");
    }
}