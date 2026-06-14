using MassTransit.Transports;
using StackExchange.Redis;

namespace RedisTransport.Transport.Middleware;

internal interface IRedisClientContextSupervisor : ITransportSupervisor<RedisClientContext>;

internal sealed class RedisClientContextSupervisor(IConnectionMultiplexer multiplexer)
    : TransportPipeContextSupervisor<RedisClientContext>(new RedisClientContextFactory(multiplexer)),
      IRedisClientContextSupervisor;
