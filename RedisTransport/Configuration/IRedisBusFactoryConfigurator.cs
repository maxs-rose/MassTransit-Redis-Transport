using MassTransit;
using StackExchange.Redis;

namespace RedisTransport.Configuration;

public interface IRedisBusFactoryConfigurator : IBusFactoryConfigurator<IRedisReceiveEndpointConfigurator>
{
    /// <summary>Configure Redis from a connection string.</summary>
    void Host(string connectionString);

    /// <summary>Configure Redis from pre-built <see cref="ConfigurationOptions"/>.</summary>
    void Host(ConfigurationOptions options);

    /// <summary>Configure Redis using a synchronous callback.</summary>
    void Host(Action<ConfigurationOptions> configure);

    /// <summary>
    /// Configure Redis using an async callback — use this for managed identity
    /// (e.g. <c>await options.ConfigureForAzureWithTokenCredentialAsync(credential)</c>).
    /// </summary>
    void Host(Func<ConfigurationOptions, Task> configureAsync);

    /// <summary>
    /// Parse a connection string and then apply async post-configuration — the common
    /// pattern for Azure Cache for Redis with managed identity where the endpoint is known
    /// but the token provider needs to be wired up asynchronously.
    /// </summary>
    void Host(string connectionString, Func<ConfigurationOptions, Task> configureAsync);
}
