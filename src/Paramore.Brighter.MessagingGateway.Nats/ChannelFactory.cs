using System.Threading;
using System.Threading.Tasks;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class ChannelFactory : IAmAChannelFactory, IAmAChannelFactoryWithScheduler
{
    private readonly NatsMessageConsumerFactory _consumerFactory;

    public ChannelFactory(NatsMessageConsumerFactory consumerFactory)
    {
        _consumerFactory = consumerFactory;
    }

    public IAmAMessageScheduler? Scheduler
    {
        get => _consumerFactory.Scheduler;
        set => _consumerFactory.Scheduler = value;
    }

    public IAmAChannelSync CreateSyncChannel(Subscription subscription)
    {
        return new Channel(subscription.ChannelName, subscription.RoutingKey, _consumerFactory.Create(subscription), subscription.BufferSize);
    }

    public IAmAChannelAsync CreateAsyncChannel(Subscription subscription)
    {
        return new ChannelAsync(subscription.ChannelName, subscription.RoutingKey, _consumerFactory.CreateAsync(subscription), subscription.BufferSize);
    }

    public Task<IAmAChannelAsync> CreateAsyncChannelAsync(Subscription subscription, CancellationToken ct = default)
    {
        IAmAChannelAsync channel = new ChannelAsync(subscription.ChannelName, subscription.RoutingKey, _consumerFactory.CreateAsync(subscription), subscription.BufferSize);
        return Task.FromResult(channel);
    }
}