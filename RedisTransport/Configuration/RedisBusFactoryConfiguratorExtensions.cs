using MassTransit;
using MassTransit.Configuration;
using MassTransit.Transports;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace RedisTransport.Configuration;

public static class RedisBusFactoryConfiguratorExtensions
{
    public static void UsingRedis(this IBusRegistrationConfigurator configurator,
        Action<IBusRegistrationContext, IRedisBusFactoryConfigurator>? configure = null)
    {
        configurator.SetEndpointNameFormatter(Defaults.EndpointNameFormatter);
        configurator.SetBusFactory(new RedisRegistrationBusFactory(configure));
    }
}

internal sealed class RedisRegistrationBusFactory(
    RedisBusConfiguration busConfiguration,
    Action<IBusRegistrationContext, IRedisBusFactoryConfigurator>? configure)
    : TransportRegistrationBusFactory<IRedisReceiveEndpointConfigurator>(busConfiguration.HostConfiguration)
{
    public RedisRegistrationBusFactory(Action<IBusRegistrationContext, IRedisBusFactoryConfigurator>? configure)
        : this(RedisBusConfiguration.Create(), configure)
    {
    }

    public override IBusInstance CreateBus(IBusRegistrationContext context, IEnumerable<IBusInstanceSpecification> specifications, string busName)
    {
        busConfiguration.HostConfiguration.Multiplexer = context.GetRequiredService<IConnectionMultiplexer>();

        var configurator = new RedisBusFactoryConfigurator(busConfiguration);
        configurator.UseRawJsonSerializer(RawSerializerOptions.CopyHeaders, true);

        return CreateBus(configurator, context, configure, specifications);
    }
}