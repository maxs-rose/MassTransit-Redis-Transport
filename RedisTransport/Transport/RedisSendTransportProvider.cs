using MassTransit.Transports;
using RedisTransport.Transport.Configuration;

namespace RedisTransport.Transport;

public sealed class RedisSendTransportProvider(IRedisHostConfiguration hostConfiguration, ReceiveEndpointContext context) : ISendTransportProvider
{
    public Uri NormalizeAddress(Uri address)
    {
        return address;
    }

    public Task<ISendTransport> GetSendTransport(Uri address)
    {
        var endpointAddress = new RedisEndpointAddress(hostConfiguration.HostAddress, address);
        ISendTransport transport = new RedisSendTransportContext(hostConfiguration, context, endpointAddress.Name, endpointAddress.Type);
        return Task.FromResult(transport);
    }
}

public sealed class RedisPublishTransportProvider(IRedisHostConfiguration hostConfiguration, ReceiveEndpointContext context) : IPublishTransportProvider
{
    public Task<ISendTransport> GetPublishTransport<T>(Uri? publishAddress) where T : class
    {
        var topicName = TopicName(typeof(T));
        ISendTransport transport = new RedisSendTransportContext(hostConfiguration, context, topicName, RedisEndpointAddress.AddressType.Topic);
        return Task.FromResult(transport);
    }

    private static string TopicName(Type type)
    {
        return MessageTypeNameFormatter.Format(type);
    }
}

internal static class MessageTypeNameFormatter
{
    public static string Format(Type type)
    {
        var ns = type.Namespace ?? "";
        return string.IsNullOrEmpty(ns) ? type.Name : $"{ns}.{type.Name}";
    }
}