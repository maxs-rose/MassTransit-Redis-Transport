using System.Diagnostics.CodeAnalysis;
using MassTransit;
using MassTransit.Transports;

namespace RedisTransport.Transport;

public sealed class RedisHeaderProvider(RedisTransportMessage message) : IHeaderProvider
{
    public IEnumerable<KeyValuePair<string, object>> GetAll()
    {
        return message.GetHeaders().GetAll();
    }

    public bool TryGetHeader(string key, [NotNullWhen(true)] out object? value)
    {
        switch (key)
        {
            case MessageHeaders.ContentType:
                value = message.ContentType;
                return value != null;
            case MessageHeaders.MessageType:
                value = message.MessageType;
                return value != null;
            case MessageHeaders.MessageId:
                value = message.MessageId;
                return value != null;
            case MessageHeaders.CorrelationId:
                value = message.CorrelationId;
                return value != null;
            case MessageHeaders.ConversationId:
                value = message.ConversationId;
                return value != null;
            case MessageHeaders.RequestId:
                value = message.RequestId;
                return value != null;
            case MessageHeaders.InitiatorId:
                value = message.InitiatorId;
                return value != null;
            case MessageHeaders.SourceAddress:
                value = message.SourceAddress;
                return value != null;
            case MessageHeaders.ResponseAddress:
                value = message.ResponseAddress;
                return value != null;
            case MessageHeaders.FaultAddress:
                value = message.FaultAddress;
                return value != null;
            case MessageHeaders.TransportMessageId:
                value = message.TransportMessageId;
                return true;
        }

        return message.GetHeaders().TryGetHeader(key, out value);
    }
}
