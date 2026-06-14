using MassTransit;

namespace TestTransport.Consumers;

public sealed record SentMessage(Guid data);

public sealed record Response(Guid data);

public sealed class SendConsumer(ILogger<SendConsumer> logger) : IConsumer<SentMessage>
{
    public async Task Consume(ConsumeContext<SentMessage> context)
    {
        logger.LogInformation("Consumed {SentMessage}", context.Message);
        await context.RespondAsync(new Response(context.Message.data));
    }
}