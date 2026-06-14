using MassTransit;
using RedisTransport.Transport.Configuration;

namespace TestTransport.Consumers;

public sealed class Ping2Consumer(ILogger<Ping2Consumer> logger) : IConsumer<Ping>
{
    public Task Consume(ConsumeContext<Ping> context)
    {
        logger.LogInformation("Consumed {Ping}", context.Message);

        return Task.Delay(TimeSpan.FromSeconds(5));
    }

    public sealed class Ping2ConsumerDefinition : ConsumerDefinition<Ping2Consumer>
    {
        protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator, IConsumerConfigurator<Ping2Consumer> consumerConfigurator, IRegistrationContext context)
        {
            if (endpointConfigurator is IRedisReceiveEndpointConfigurator redisEndpointConfigurator)
                redisEndpointConfigurator.MessageTimeToLive = TimeSpan.FromSeconds(1);
        }
    }
}