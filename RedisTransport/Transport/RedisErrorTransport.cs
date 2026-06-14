using System.Text.Json;
using MassTransit;
using MassTransit.Serialization;
using MassTransit.Transports;
using RedisTransport.Transport.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisErrorTransport(string errorQueueName, IRedisHostConfiguration hostConfiguration) : IErrorTransport
{
    public async Task Send(ExceptionReceiveContext context)
    {
        if (!context.TryGetPayload(out RedisTransportMessage? message) || message is null)
            return;

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

        return new NameValueEntry[]
        {
            new("transport-message-id", message.TransportMessageId.ToString("D")),
            new("body", message.Body ?? string.Empty),
            new("content-type", message.ContentType ?? string.Empty),
            new("message-type", message.MessageType ?? string.Empty),
            new("message-id", message.MessageId?.ToString("D") ?? string.Empty),
            new("correlation-id", message.CorrelationId?.ToString("D") ?? string.Empty),
            new("conversation-id", message.ConversationId?.ToString("D") ?? string.Empty),
            new("request-id", message.RequestId?.ToString("D") ?? string.Empty),
            new("initiator-id", message.InitiatorId?.ToString("D") ?? string.Empty),
            new("source-address", message.SourceAddress?.ToString() ?? context.InputAddress.ToString()),
            new("destination-address", message.DestinationAddress?.ToString() ?? string.Empty),
            new("response-address", message.ResponseAddress?.ToString() ?? string.Empty),
            new("fault-address", message.FaultAddress?.ToString() ?? string.Empty),
            new("sent-time", message.SentTime?.ToString("O") ?? string.Empty),
            new("expiration-time", string.Empty),
            new("headers", headersJson ?? string.Empty),
            new("partition-key", message.PartitionKey ?? string.Empty),
            new("routing-key", message.RoutingKey ?? string.Empty)
        };
    }
}