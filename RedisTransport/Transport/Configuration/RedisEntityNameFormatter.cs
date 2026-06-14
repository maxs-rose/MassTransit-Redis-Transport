using MassTransit.Transports;

namespace RedisTransport.Transport.Configuration;

internal class RedisEntityNameFormatter(bool includeNamespace) : IMessageNameFormatter
{
    private readonly IMessageNameFormatter _formatter = new DefaultMessageNameFormatter("::", "--", ":", "-", includeNamespace);

    public RedisEntityNameFormatter() : this(true)
    {
    }

    public string GetMessageName(Type type)
    {
        return _formatter.GetMessageName(type);
    }
}