namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessageConsumerFactory(
    NatsMessagingGatewayConsumerConfiguration configuration,
    IAmAMessageScheduler? scheduler = null) : IAmAMessageConsumerFactory
{
    private readonly NatsMessagingGatewayConsumerConfiguration _configuration = configuration;
    private IAmAMessageScheduler? _scheduler = scheduler;

    public IAmAMessageScheduler? Scheduler
    {
        get => _scheduler;
        set => _scheduler = value;
    }

    public IAmAMessageConsumerSync Create(Subscription subscription)
    {
        var deadLetterRoutingKey = (subscription as IUseBrighterDeadLetterSupport)?.DeadLetterRoutingKey;
        var invalidMessageRoutingKey = (subscription as IUseBrighterInvalidMessageSupport)?.InvalidMessageRoutingKey;

        return new NatsMessageConsumer(_configuration, subscription, _scheduler, deadLetterRoutingKey, invalidMessageRoutingKey);
    }

    public IAmAMessageConsumerAsync CreateAsync(Subscription subscription)
    {
        var deadLetterRoutingKey = (subscription as IUseBrighterDeadLetterSupport)?.DeadLetterRoutingKey;
        var invalidMessageRoutingKey = (subscription as IUseBrighterInvalidMessageSupport)?.InvalidMessageRoutingKey;

        return new NatsMessageConsumer(_configuration, subscription, _scheduler, deadLetterRoutingKey, invalidMessageRoutingKey);
    }
}