using System;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsSubscription(
    SubscriptionName subscriptionName,
    ChannelName channelName,
    RoutingKey routingKey,
    Type? requestType = null,
    Func<Message, Type>? getRequestType = null,
    int bufferSize = 1,
    int noOfPerformers = 1,
    TimeSpan? timeOut = null,
    int requeueCount = -1,
    TimeSpan? requeueDelay = null,
    int unacceptableMessageLimit = 0,
    TimeSpan? unacceptableMessageLimitWindow = null,
    MessagePumpType messagePumpType = MessagePumpType.Unknown,
    IAmAChannelFactory? channelFactory = null,
    OnMissingChannel makeChannels = OnMissingChannel.Create,
    TimeSpan? emptyChannelDelay = null,
    TimeSpan? channelFailureDelay = null,
    RoutingKey? deadLetterRoutingKey = null,
    RoutingKey? invalidMessageRoutingKey = null) : Subscription(
        subscriptionName,
        channelName,
        routingKey,
        requestType,
        getRequestType,
        bufferSize,
        noOfPerformers,
        timeOut ?? TimeSpan.FromSeconds(1),
        requeueCount,
        requeueDelay,
        unacceptableMessageLimit,
        messagePumpType,
        channelFactory,
        makeChannels,
        emptyChannelDelay,
        channelFailureDelay,
        unacceptableMessageLimitWindow), IUseBrighterDeadLetterSupport, IUseBrighterInvalidMessageSupport
{
    public override Type ChannelFactoryType => typeof(ChannelFactory);

    public RoutingKey? DeadLetterRoutingKey { get; set; } = deadLetterRoutingKey;

    public RoutingKey? InvalidMessageRoutingKey { get; set; } = invalidMessageRoutingKey;
}

public class NatsSubscription<T>(
    SubscriptionName? subscriptionName = null,
    ChannelName? channelName = null,
    RoutingKey? routingKey = null,
    Func<Message, Type>? getRequestType = null,
    int bufferSize = 1,
    int noOfPerformers = 1,
    TimeSpan? timeOut = null,
    int requeueCount = -1,
    TimeSpan? requeueDelay = null,
    int unacceptableMessageLimit = 0,
    TimeSpan? unacceptableMessageLimitWindow = null,
    MessagePumpType messagePumpType = MessagePumpType.Proactor,
    IAmAChannelFactory? channelFactory = null,
    OnMissingChannel makeChannels = OnMissingChannel.Create,
    TimeSpan? emptyChannelDelay = null,
    TimeSpan? channelFailureDelay = null,
    RoutingKey? deadLetterRoutingKey = null,
    RoutingKey? invalidMessageRoutingKey = null) : NatsSubscription(
        subscriptionName ?? new SubscriptionName(typeof(T).FullName!),
        channelName ?? new ChannelName(typeof(T).FullName!),
        routingKey ?? new RoutingKey(typeof(T).FullName!),
        typeof(T),
        getRequestType,
        bufferSize,
        noOfPerformers,
        timeOut,
        requeueCount,
        requeueDelay,
        unacceptableMessageLimit,
        unacceptableMessageLimitWindow,
        messagePumpType,
        channelFactory,
        makeChannels,
        emptyChannelDelay,
        channelFailureDelay,
        deadLetterRoutingKey,
        invalidMessageRoutingKey) where T : IRequest;
