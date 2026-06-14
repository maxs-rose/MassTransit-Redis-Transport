using System.Globalization;
using System.Text.Json;
using MassTransit;
using MassTransit.Serialization;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal sealed class RedisTransportMessage
{
    private SendHeaders? _headers;

    public string StreamKey { get; set; } = null!;
    public string EntryId { get; set; } = null!;
    public int DeliveryCount { get; set; }

    public Guid TransportMessageId { get; set; }
    public Guid? MessageId { get; set; }
    public Guid? RequestId { get; set; }
    public Guid? CorrelationId { get; set; }
    public Guid? ConversationId { get; set; }
    public Guid? InitiatorId { get; set; }

    public string? ContentType { get; set; }
    public string? MessageType { get; set; }
    public string? Body { get; set; }

    public string? HeadersJson { get; set; }
    public string? PartitionKey { get; set; }
    public string? RoutingKey { get; set; }

    public Uri? SourceAddress { get; set; }
    public Uri? DestinationAddress { get; set; }
    public Uri? ResponseAddress { get; set; }
    public Uri? FaultAddress { get; set; }

    public DateTime? SentTime { get; set; }
    public DateTime? ExpirationTime { get; set; }

    public SendHeaders GetHeaders()
    {
        return _headers ??= DeserializeHeaders(HeadersJson);
    }

    private static SendHeaders DeserializeHeaders(string? json)
    {
        var headers = new DictionarySendHeaders();

        if (string.IsNullOrEmpty(json))
            return headers;

        var elements = JsonSerializer.Deserialize<IEnumerable<KeyValuePair<string, object>>>(json, SystemTextJsonMessageSerializer.Options);
        if (elements != null)
            foreach (var element in elements)
                headers.Set(element.Key, element.Value);

        return headers;
    }

    internal static RedisTransportMessage FromStreamEntry(string key, StreamEntry entry)
    {
        return new RedisTransportMessage
        {
            StreamKey = key,
            EntryId = entry.Id.ToString(),
            DeliveryCount = 0,
            TransportMessageId = GetGuid(nameof(TransportMessageId), entry) ?? Guid.Empty,
            MessageId = GetGuid(nameof(MessageId), entry),
            RequestId = GetGuid(nameof(RequestId), entry),
            CorrelationId = GetGuid(nameof(CorrelationId), entry),
            ConversationId = GetGuid(nameof(ConversationId), entry),
            InitiatorId = GetGuid(nameof(InitiatorId), entry),
            ContentType = GetString(nameof(ContentType), entry),
            MessageType = GetString(nameof(MessageType), entry),
            Body = GetString(nameof(Body), entry),
            HeadersJson = GetString(nameof(HeadersJson), entry),
            PartitionKey = GetString(nameof(PartitionKey), entry),
            RoutingKey = GetString(nameof(RoutingKey), entry),
            SourceAddress = GetUri(nameof(SourceAddress), entry),
            DestinationAddress = GetUri(nameof(DestinationAddress), entry),
            ResponseAddress = GetUri(nameof(ResponseAddress), entry),
            FaultAddress = GetUri(nameof(FaultAddress), entry),
            SentTime = GetDateTime(nameof(SentTime), entry),
            ExpirationTime = GetDateTime(nameof(ExpirationTime), entry)
        };
    }

    private static DateTime? GetDateTime(string field, StreamEntry entry)
    {
        var s = GetString(field, entry);
        return string.IsNullOrEmpty(s) ? null : DateTime.TryParse(s, null, DateTimeStyles.RoundtripKind, out var d) ? d : null;
    }

    private static Uri? GetUri(string field, StreamEntry entry)
    {
        var s = GetString(field, entry);
        return string.IsNullOrEmpty(s) ? null : Uri.TryCreate(s, UriKind.Absolute, out var u) ? u : null;
    }

    private static Guid? GetGuid(string field, StreamEntry entry)
    {
        var s = GetString(field, entry);
        return string.IsNullOrEmpty(s) ? null : Guid.TryParse(s, out var g) ? g : null;
    }

    private static string? GetString(string field, StreamEntry entry)
    {
        var value = entry[field];
        return value.IsNullOrEmpty ? null : (string?)value;
    }
}