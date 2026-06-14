using MassTransit;
using MassTransit.Configuration;
using RedisTransport.Configuration;

namespace RedisTransport.Transport;

internal sealed class RedisReceiveEndpointBuilder(IRedisReceiveEndpointConfiguration configuration)
    : ReceiveEndpointBuilder(configuration)
{
    private readonly HashSet<Type> _subscribedTypes = new();

    public IReadOnlyCollection<Type> SubscribedMessageTypes => _subscribedTypes;

    public override ConnectHandle ConnectConsumePipe<T>(IPipe<ConsumeContext<T>> pipe, ConnectPipeOptions options)
    {
        if (configuration.ConfigureConsumeTopology && options.HasFlag(ConnectPipeOptions.ConfigureConsumeTopology))
            _subscribedTypes.Add(typeof(T));

        return base.ConnectConsumePipe(pipe, options);
    }
}