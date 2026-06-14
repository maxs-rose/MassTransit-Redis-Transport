using MassTransit;

namespace TestTransport.Consumers;

public sealed record Ping(string Message);

public sealed class PingConsumer(ILogger<PingConsumer> logger) : IConsumer<Ping>
{
    public static readonly Guid Id = Guid.NewGuid();

    public Task Consume(ConsumeContext<Ping> context)
    {
        logger.LogInformation("{Id} Consumed {Ping}", Id, context.Message);

        return Task.CompletedTask;
    }
}