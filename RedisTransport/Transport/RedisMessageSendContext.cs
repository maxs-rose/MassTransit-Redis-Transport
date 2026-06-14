using System.Text.Json;
using MassTransit;
using MassTransit.Context;
using MassTransit.Serialization;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisMessageSendContext<T>(T message, CancellationToken cancellationToken) : MessageSendContext<T>(message, cancellationToken)
    where T : class
{
    public Guid TransportMessageId { get; } = NewId.NextGuid();

    public string? PartitionKey { get; set; }
    public string? RoutingKey { get; set; }

    internal NameValueEntry[] RedisMessage()
    {
        var headers = Headers.GetAll().ToList();
        var headersJson = headers.Count > 0
            ? JsonSerializer.Serialize(headers, SystemTextJsonMessageSerializer.Options)
            : null;

        DateTime? expirationTime = TimeToLive.HasValue ? DateTime.UtcNow + TimeToLive.Value : null;

        return
        [
            new NameValueEntry(nameof(RedisTransportMessage.TransportMessageId), TransportMessageId.ToString("D")),
            new NameValueEntry(nameof(RedisTransportMessage.Body), Body.GetString() ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.ContentType), ContentType?.MediaType ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.MessageType), string.Join(";", SupportedMessageTypes)),
            new NameValueEntry(nameof(RedisTransportMessage.MessageId), MessageId?.ToString("D") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.CorrelationId), CorrelationId?.ToString("D") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.ConversationId), ConversationId?.ToString("D") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.RequestId), RequestId?.ToString("D") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.InitiatorId), InitiatorId?.ToString("D") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.SourceAddress), SourceAddress?.ToString() ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.DestinationAddress), DestinationAddress?.ToString() ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.ResponseAddress), ResponseAddress?.ToString() ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.FaultAddress), FaultAddress?.ToString() ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.SentTime), SentTime?.ToString("O") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.ExpirationTime), expirationTime?.ToString("O") ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.HeadersJson), headersJson ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.PartitionKey), PartitionKey ?? string.Empty),
            new NameValueEntry(nameof(RedisTransportMessage.RoutingKey), RoutingKey ?? string.Empty)
        ];
    }
}