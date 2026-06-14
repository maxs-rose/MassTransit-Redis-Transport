using MassTransit;

namespace RedisTransport.Transport.Configuration;

public interface IRedisBusFactoryConfigurator : IBusFactoryConfigurator<IRedisReceiveEndpointConfigurator>;