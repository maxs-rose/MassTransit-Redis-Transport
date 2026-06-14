using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("Redis");

builder.AddProject<TestTransport>("RedisTransportTest")
    .WithEnvironment("UseWorker", "true")
    .WaitFor(redis)
    .WithReference(redis);

// builder.AddProject<TestTransport>("RedisTransportTest2")
//     .WithEnvironment("UseWorker", "false")
//     .WaitFor(redis)
//     .WithReference(redis);

builder.Build().Run();