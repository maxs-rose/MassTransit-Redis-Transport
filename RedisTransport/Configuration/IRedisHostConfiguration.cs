using MassTransit;
using MassTransit.Configuration;
using StackExchange.Redis;

namespace RedisTransport.Configuration;

internal interface IRedisHostConfiguration : IHostConfiguration, IReceiveConfigurator<IRedisReceiveEndpointConfigurator>
{
    IConnectionMultiplexer Multiplexer { get; set; }

    IRedisReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(
        RedisReceiveSettings settings,
        IRedisEndpointConfiguration endpointConfiguration,
        Action<IRedisReceiveEndpointConfigurator>? configure = null);

    IRedisReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(
        string queueName,
        Action<IRedisReceiveEndpointConfigurator>? configure = null);
}