using MassTransit.Configuration;
using MassTransit.Topology;

namespace RedisTransport.Transport.Configuration;

public static class RedisBusFactory
{
    public static IMessageTopologyConfigurator CreateMessageTopology()
    {
        return new MessageTopology(Defaults.EntityNameFormatter);
    }
}