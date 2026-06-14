using MassTransit;
using MassTransit.Transports;

namespace RedisTransport.Configuration;

internal sealed class RedisEndpointNameFormatter(IMessageNameFormatter messageNameFormatter) : DefaultEndpointNameFormatter(true)
{
    protected override string FormatName(Type type)
    {
        return messageNameFormatter.GetMessageName(type);
    }
}