using MassTransit;
using MassTransit.Transports;

namespace RedisTransport.Transport.Configuration;

public static class Defaults
{
    internal static readonly IMessageNameFormatter MessageNameFormatter = new RedisEntityNameFormatter();
    internal static readonly IEntityNameFormatter EntityNameFormatter = new MessageNameFormatterEntityNameFormatter(MessageNameFormatter);
    internal static readonly IEndpointNameFormatter EndpointNameFormatter = new RedisEndpointNameFormatter(MessageNameFormatter);

    public static readonly TimeSpan TemporaryAutoDeleteOnIdle = TimeSpan.FromMinutes(1);
}