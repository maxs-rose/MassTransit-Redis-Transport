using MassTransit;
using MassTransit.Agents;
using StackExchange.Redis;

namespace RedisTransport.Transport.Middleware;

internal sealed class RedisClientContextFactory(IConnectionMultiplexer multiplexer) : IPipeContextFactory<RedisClientContext>
{
    IPipeContextAgent<RedisClientContext> IPipeContextFactory<RedisClientContext>.CreateContext(ISupervisor supervisor)
    {
        IAsyncPipeContextAgent<RedisClientContext> asyncContext = supervisor.AddAsyncContext<RedisClientContext>();
        _ = asyncContext.Created(new RedisClientContextImpl(multiplexer, supervisor.Stopping));
        return asyncContext;
    }

    IActivePipeContextAgent<RedisClientContext> IPipeContextFactory<RedisClientContext>.CreateActiveContext(
        ISupervisor supervisor, PipeContextHandle<RedisClientContext> context, CancellationToken cancellationToken)
    {
        return supervisor.AddActiveContext(context, GetActiveContext(context.Context, cancellationToken));
    }

    static Task<RedisClientContext> GetActiveContext(Task<RedisClientContext> context, CancellationToken cancellationToken) =>
        context.IsCompletedSuccessfully ? Task.FromResult(context.Result) : context.WaitAsync(cancellationToken);
}
