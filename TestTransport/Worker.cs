using MassTransit;
using TestTransport.Consumers;
using Response = TestTransport.Consumers.Response;

namespace TestTransport;

public class Worker(ILogger<Worker> logger, IServiceProvider serviceProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await using var scope = serviceProvider.CreateAsyncScope();
            var bus = scope.ServiceProvider.GetRequiredService<IBus>();
            logger.LogInformation("Sending Ping");
            await bus.Publish(new Ping($"{Guid.NewGuid()}"), stoppingToken);

            await Task.Delay(1000, stoppingToken);
        }
    }
}

public class SendWorker(ILogger<Worker> logger, IServiceProvider serviceProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await using var scope = serviceProvider.CreateAsyncScope();
            var bus = scope.ServiceProvider.GetRequiredService<IRequestClient<SentMessage>>();


            var message = new SentMessage(Guid.NewGuid());

            logger.LogInformation("Sending {Message}", message);

            var rec = await bus.GetResponse<Response>(message, stoppingToken);

            logger.LogInformation("Received {Response}", rec.Message);

            await Task.Delay(1000, stoppingToken);
        }
    }
}