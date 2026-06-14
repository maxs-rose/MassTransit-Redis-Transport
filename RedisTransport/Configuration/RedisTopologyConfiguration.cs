using System.Diagnostics.CodeAnalysis;
using MassTransit;
using MassTransit.Configuration;
using MassTransit.Topology;

namespace RedisTransport.Configuration;

internal sealed class RedisTopologyConfiguration : IRedisTopologyConfiguration
{
    public RedisTopologyConfiguration(IMessageTopologyConfigurator messageTopology)
    {
        var publish = new RedisPublishTopology();
        publish.ConnectPublishTopologyConfigurationObserver(new DelegatePublishTopologyConfigurationObserver(GlobalTopology.Publish));

        var send = new RedisSendTopology();
        send.ConnectSendTopologyConfigurationObserver(new DelegateSendTopologyConfigurationObserver(GlobalTopology.Send));
        send.TryAddConvention(new RoutingKeySendTopologyConvention());
        send.TryAddConvention(new PartitionKeySendTopologyConvention());

        publish.ConnectPublishTopologyConfigurationObserver(new PublishToSendTopologyConfigurationObserver(send));

        Message = messageTopology;
        Send = send;
        Publish = publish;
        Consume = new RedisConsumeTopology();
    }

    public RedisTopologyConfiguration(IRedisTopologyConfiguration configuration)
    {
        Message = configuration.Message;
        Send = configuration.Send;
        Publish = configuration.Publish;
        Consume = configuration.Consume;
    }

    public IEnumerable<ValidationResult> Validate()
    {
        return Send.Validate().Concat(Publish.Validate()).Concat(Consume.Validate());
    }

    public IMessageTopologyConfigurator Message { get; }
    public ISendTopologyConfigurator Send { get; }
    public IPublishTopologyConfigurator Publish { get; }
    public IConsumeTopologyConfigurator Consume { get; }
}

public sealed class RedisSendTopology : SendTopology;

public sealed class RedisPublishTopology : PublishTopology
{
    protected override IMessagePublishTopologyConfigurator CreateMessageTopology<T>()
    {
        var topology = new RedisMessagePublishTopology<T>(this);
        OnMessageTopologyCreated(topology);
        return topology;
    }
}

public sealed class RedisMessagePublishTopology<T>(IPublishTopology publishTopology)
    : MessagePublishTopology<T>(publishTopology) where T : class
{
    private readonly string _topicName = RedisMessageTypeFormatter.Format(typeof(T));

    public override bool TryGetPublishAddress(Uri baseAddress, [NotNullWhen(true)] out Uri? publishAddress)
    {
        publishAddress = new Uri(baseAddress, $"/{_topicName}?type=topic");
        return true;
    }
}

public sealed class RedisConsumeTopology : ConsumeTopology;