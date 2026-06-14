using MassTransit.Transports;
using RedisTransport.Configuration;

namespace RedisTransport.Transport;

internal sealed class RedisSendTransportProvider(IRedisHostConfiguration hostConfiguration, ReceiveEndpointContext context)
    : ISendTransportProvider
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

internal sealed class RedisPublishTransportProvider(IRedisHostConfiguration hostConfiguration, ReceiveEndpointContext context)
    : IPublishTransportProvider
{
    public Task<ISendTransport> GetPublishTransport<T>(Uri? publishAddress) where T : class
    {
        var topicName = RedisMessageTypeFormatter.Format(typeof(T));
        ISendTransport transport = new RedisSendTransportContext(hostConfiguration, context, topicName, RedisEndpointAddress.AddressType.Topic);
        return Task.FromResult(transport);
    }
}