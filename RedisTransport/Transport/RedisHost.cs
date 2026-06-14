using MassTransit;
using MassTransit.Transports;
using RedisTransport.Configuration;

namespace RedisTransport.Transport;

internal sealed class RedisHost : BaseHost, IHost<IRedisReceiveEndpointConfigurator>
{
    private readonly IRedisHostConfiguration _hostConfiguration;

    public RedisHost(IRedisHostConfiguration hostConfiguration, IBusTopology busTopology)
        : base(hostConfiguration, busTopology)
    {
        _hostConfiguration = hostConfiguration;
    }

    public override HostReceiveEndpointHandle ConnectReceiveEndpoint(IEndpointDefinition definition,
        IEndpointNameFormatter? endpointNameFormatter, Action<IReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        var queueName = definition.GetEndpointName(endpointNameFormatter ?? DefaultEndpointNameFormatter.Instance);
        return ConnectReceiveEndpoint(queueName, configureEndpoint);
    }

    public override HostReceiveEndpointHandle ConnectReceiveEndpoint(string queueName,
        Action<IReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        return ConnectReceiveEndpoint(queueName, configureEndpoint == null ? null : (Action<IRedisReceiveEndpointConfigurator>)(c => configureEndpoint(c)));
    }

    public HostReceiveEndpointHandle ConnectReceiveEndpoint(IEndpointDefinition definition,
        IEndpointNameFormatter? endpointNameFormatter = null, Action<IRedisReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        var queueName = definition.GetEndpointName(endpointNameFormatter ?? DefaultEndpointNameFormatter.Instance);
        return ConnectReceiveEndpoint(queueName, configureEndpoint);
    }

    public HostReceiveEndpointHandle ConnectReceiveEndpoint(string queueName,
        Action<IRedisReceiveEndpointConfigurator>? configureEndpoint = null)
    {
        LogContext.SetCurrentIfNull(_hostConfiguration.LogContext);

        var configuration = _hostConfiguration.CreateReceiveEndpointConfiguration(queueName, configureEndpoint);
        configuration.Validate().ThrowIfContainsFailure("The receive endpoint configuration is invalid:");
        configuration.Build(this);

        return ReceiveEndpoints.Start(queueName);
    }

    protected override void Probe(ProbeContext context)
    {
        context.Set(new { Type = "Redis", _hostConfiguration.HostAddress });
    }
}