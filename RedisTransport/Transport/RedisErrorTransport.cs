using System.Text.Json;
using MassTransit;
using MassTransit.Serialization;
using MassTransit.Transports;
using RedisTransport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisErrorTransport(string errorQueueName, IRedisHostConfiguration hostConfiguration) : IErrorTransport
{
    public async Task Send(ExceptionReceiveContext context)
    {
        if (!context.TryGetPayload(out RedisTransportMessage? message) || message is null)
            return;

        LogContext.Warning?.Log("Moving message {MessageId} (entry {EntryId}) from {Stream} to error queue {ErrorQueue}",
            message.MessageId, message.EntryId, message.StreamKey, errorQueueName);

        var entries = BuildEntries(message, context);
        var db = hostConfiguration.Multiplexer.GetDatabase();

        await db.StreamAddAsync(RedisKeys.QueueStream(errorQueueName), entries).ConfigureAwait(false);
        await hostConfiguration.Multiplexer.GetSubscriber()
            .PublishAsync(RedisChannel.Literal(RedisKeys.QueueNotify(errorQueueName)), RedisValue.EmptyString)
            .ConfigureAwait(false);
    }

    private static NameValueEntry[] BuildEntries(RedisTransportMessage message, ExceptionReceiveContext context)
    {
        var combined = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var kvp in message.GetHeaders().GetAll())
            combined[kvp.Key] = kvp.Value;
        foreach (var kvp in context.ExceptionHeaders.GetAll())
            combined[kvp.Key] = kvp.Value;

        var headersJson = combined.Count > 0
            ? JsonSerializer.Serialize(combined, SystemTextJsonMessageSerializer.Options)
            : null;

        return
        [
            new NameValueEntry("transport-message-id", message.TransportMessageId.ToString("D")),
            new NameValueEntry("body", message.Body ?? string.Empty),
            new NameValueEntry("content-type", message.ContentType ?? string.Empty),
            new NameValueEntry("message-type", message.MessageType ?? string.Empty),
            new NameValueEntry("message-id", message.MessageId?.ToString("D") ?? string.Empty),
            new NameValueEntry("correlation-id", message.CorrelationId?.ToString("D") ?? string.Empty),
            new NameValueEntry("conversation-id", message.ConversationId?.ToString("D") ?? string.Empty),
            new NameValueEntry("request-id", message.RequestId?.ToString("D") ?? string.Empty),
            new NameValueEntry("initiator-id", message.InitiatorId?.ToString("D") ?? string.Empty),
            new NameValueEntry("source-address", message.SourceAddress?.ToString() ?? context.InputAddress.ToString()),
            new NameValueEntry("destination-address", message.DestinationAddress?.ToString() ?? string.Empty),
            new NameValueEntry("response-address", message.ResponseAddress?.ToString() ?? string.Empty),
            new NameValueEntry("fault-address", message.FaultAddress?.ToString() ?? string.Empty),
            new NameValueEntry("sent-time", message.SentTime?.ToString("O") ?? string.Empty),
            new NameValueEntry("expiration-time", string.Empty),
            new NameValueEntry("headers", headersJson ?? string.Empty),
            new NameValueEntry("partition-key", message.PartitionKey ?? string.Empty),
            new NameValueEntry("routing-key", message.RoutingKey ?? string.Empty)
        ];
    }
}