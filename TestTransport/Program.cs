using MassTransit;
using MassTransit.Logging;
using OpenTelemetry;
using RedisTransport.Configuration;
using TestTransport;
using TestTransport.Consumers;

var builder = Host.CreateApplicationBuilder(args);

if (builder.Configuration.GetValue<bool>("UseWorker"))
    builder.Services.AddHostedService<Worker>();
// builder.Services.AddHostedService<SendWorker>();

builder.Services.AddMassTransit(x =>
{
    // x.AddConsumers(typeof(Program).Assembly);
    x.AddConsumer<PingConsumer>();
    // x.AddConsumer<Ping2Consumer, Ping2Consumer.Ping2ConsumerDefinition>();
    // x.AddConsumer<SendConsumer>();

    x.UsingRedis((context, cfg) =>
    {
        var connectionString = context.GetRequiredService<IConfiguration>().GetConnectionString("Redis")!;
        cfg.Host(connectionString);
        cfg.ConfigureEndpoints(context);
    });
});

builder.Services.AddOpenTelemetry()
    .UseOtlpExporter()
    .WithTracing(o =>
        o.AddSource(DiagnosticHeaders.DefaultListenerName));

var host = builder.Build();
host.Run();