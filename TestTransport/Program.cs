using MassTransit;
using MassTransit.Logging;
using OpenTelemetry;
using RedisTransport.Telemetry;
using RedisTransport.Configuration;
using TestTransport;
using TestTransport.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.AddRedisClient("Redis", o => { o.DisableTracing = true; });

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

builder.Services.AddOpenTelemetry()
    .UseOtlpExporter()
    .WithTracing(o =>
        o.AddSource(DiagnosticHeaders.DefaultListenerName)
            .AddSource(Otel.DefaultListenerName));

var host = builder.Build();
host.Run();