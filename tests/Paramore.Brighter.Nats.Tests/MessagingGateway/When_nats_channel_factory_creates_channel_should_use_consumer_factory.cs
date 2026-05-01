#region Licence
/* The MIT License (MIT)
Copyright © 2026 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.Nats;
using Xunit;

namespace Paramore.Brighter.Nats.Tests.MessagingGateway;

public class When_nats_channel_factory_creates_channel_should_use_consumer_factory
{
    private readonly NatsMessageConsumerFactory _consumerFactory;
    private readonly ChannelFactory _channelFactory;

    private readonly Subscription _subscription = new(
        subscriptionName: new SubscriptionName("test"),
        channelName: new ChannelName("test.queue"),
        routingKey: new RoutingKey("test.key"),
        requestType: typeof(Command),
        messagePumpType: MessagePumpType.Reactor);

    public When_nats_channel_factory_creates_channel_should_use_consumer_factory()
    {
        var configuration = new NatsMessagingGatewayConsumerConfiguration
        {
            Url = "nats://localhost:4222",
            SubjectPrefix = "brighter",
            ConnectionName = "test-client"
        };

        _consumerFactory = new NatsMessageConsumerFactory(configuration);
        _channelFactory = new ChannelFactory(_consumerFactory);
    }

    [Fact]
    public void Should_create_sync_channel()
    {
        var channel = _channelFactory.CreateSyncChannel(_subscription);

        Assert.NotNull(channel);
        Assert.IsType<Channel>(channel);
    }

    [Fact]
    public void Should_create_async_channel()
    {
        var channel = _channelFactory.CreateAsyncChannel(_subscription);

        Assert.NotNull(channel);
        Assert.IsType<ChannelAsync>(channel);
    }

    [Fact]
    public async Task Should_create_async_channel_async()
    {
        var channel = await _channelFactory.CreateAsyncChannelAsync(_subscription);

        Assert.NotNull(channel);
        Assert.IsType<ChannelAsync>(channel);
    }

    [Fact]
    public void Should_implement_channel_factory_with_scheduler()
    {
        Assert.IsAssignableFrom<IAmAChannelFactoryWithScheduler>(_channelFactory);
    }

    [Fact]
    public void Should_accept_scheduler_property()
    {
        var scheduler = new StubMessageScheduler();

        ((IAmAChannelFactoryWithScheduler)_channelFactory).Scheduler = scheduler;

        Assert.Equal(scheduler, ((IAmAChannelFactoryWithScheduler)_channelFactory).Scheduler);
    }

    private sealed class StubMessageScheduler : IAmAMessageScheduler;
}
