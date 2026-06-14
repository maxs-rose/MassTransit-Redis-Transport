using MassTransit;

namespace RedisTransport.Configuration;

public interface IRedisBusFactoryConfigurator : IBusFactoryConfigurator<IRedisReceiveEndpointConfigurator>;