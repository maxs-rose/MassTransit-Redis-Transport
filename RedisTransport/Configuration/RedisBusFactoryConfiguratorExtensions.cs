using System.Net;
using MassTransit;
using MassTransit.Configuration;
using MassTransit.Transports;
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
    Action<IBusRegistrationContext, IRedisBusFactoryConfigurator>? configure
) : TransportRegistrationBusFactory<IRedisReceiveEndpointConfigurator>(busConfiguration.HostConfiguration)
{
    public RedisRegistrationBusFactory(Action<IBusRegistrationContext, IRedisBusFactoryConfigurator>? configure)
        : this(RedisBusConfiguration.Create(), configure)
    {
    }

    public override IBusInstance CreateBus(IBusRegistrationContext context, IEnumerable<IBusInstanceSpecification> specifications, string busName)
    {
        var configurator = new RedisBusFactoryConfigurator(busConfiguration);
        configurator.UseRawJsonSerializer(RawSerializerOptions.CopyHeaders, true);

        configure?.Invoke(context, configurator);

        var multiplexer = configurator.CreateMultiplexer().GetAwaiter().GetResult();
        busConfiguration.HostConfiguration.Multiplexer = multiplexer;
        busConfiguration.HostConfiguration.SetHostAddress(DeriveHostAddress(multiplexer));

        return CreateBus(configurator, context, (Action<IBusRegistrationContext, IRedisBusFactoryConfigurator>?)null, specifications);
    }

    private static Uri DeriveHostAddress(IConnectionMultiplexer multiplexer)
    {
        var options = ConfigurationOptions.Parse(multiplexer.Configuration);
        var ep = options.EndPoints.FirstOrDefault();
        return ep switch
        {
            DnsEndPoint dns => new Uri($"redis://{dns.Host}:{dns.Port}/"),
            IPEndPoint ip => new Uri($"redis://{ip.Address}:{ip.Port}/"),
            _ => new Uri("redis://localhost/")
        };
    }
}