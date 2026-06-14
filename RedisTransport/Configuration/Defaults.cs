using MassTransit;
using MassTransit.Transports;

namespace RedisTransport.Configuration;

internal static class Defaults
{
    private static readonly IMessageNameFormatter MessageNameFormatter = new RedisEntityNameFormatter();
    public static readonly IEntityNameFormatter EntityNameFormatter = new MessageNameFormatterEntityNameFormatter(MessageNameFormatter);
    public static readonly IEndpointNameFormatter EndpointNameFormatter = new RedisEndpointNameFormatter(MessageNameFormatter);
    public static readonly TimeSpan TemporaryAutoDeleteOnIdle = TimeSpan.FromMinutes(1);
}