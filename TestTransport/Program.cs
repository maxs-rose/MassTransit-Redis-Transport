using MassTransit;
using RedisTransport.Transport.Configuration;
using TestTransport;
using TestTransport.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.AddRedisClient("Redis");

if (builder.Configuration.GetValue<bool>("UseWorker"))
    builder.Services.AddHostedService<Worker>();
// builder.Services.AddHostedService<SendWorker>();

builder.Services.AddMassTransit(x =>
{
    // x.AddConsumers(typeof(Program).Assembly);
    x.AddConsumer<PingConsumer>();
    // x.AddConsumer<Ping2Consumer, Ping2Consumer.Ping2ConsumerDefinition>();
    // x.AddConsumer<SendConsumer>();

    x.UsingRedis((context, cfg) => { cfg.ConfigureEndpoints(context); });
});

var host = builder.Build();
host.Run();