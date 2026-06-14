using MassTransit;
using MassTransit.Middleware;
using StackExchange.Redis;

namespace RedisTransport.Transport;

internal interface RedisClientContext : PipeContext
{
    IDatabase Database { get; }
    ISubscriber Subscriber { get; }
}

internal sealed class RedisClientContextImpl : BasePipeContext, RedisClientContext
{
    public RedisClientContextImpl(IConnectionMultiplexer multiplexer, CancellationToken cancellationToken)
        : base(cancellationToken)
    {
        Database = multiplexer.GetDatabase();
        Subscriber = multiplexer.GetSubscriber();
    }

    public IDatabase Database { get; }
    public ISubscriber Subscriber { get; }
}
