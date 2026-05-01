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

using System;
using Paramore.Brighter.MessagingGateway.Nats;
using Xunit;

namespace Paramore.Brighter.Nats.Tests.MessagingGateway;

public class When_nats_consumer_requeues_with_delay_should_use_scheduler
{
    [Fact]
    public void Should_delegate_to_the_scheduler()
    {
        var scheduler = new StubMessageScheduler();
        var configuration = new NatsMessagingGatewayConsumerConfiguration
        {
            Url = "nats://localhost:4222",
            SubjectPrefix = "brighter"
        };

        var subscription = new NatsSubscription<Command>(
            subscriptionName: new SubscriptionName("test-subscription"),
            channelName: new ChannelName("test-channel"),
            routingKey: new RoutingKey("orders.created"),
            messagePumpType: MessagePumpType.Reactor);

        using var consumer = new NatsMessageConsumer(configuration, subscription, scheduler);

        var message = new Message(
            new MessageHeader(messageId: Guid.NewGuid().ToString(), topic: new RoutingKey("orders.created"), messageType: MessageType.MT_COMMAND),
            new MessageBody("{}"));

        var delay = TimeSpan.FromSeconds(5);

        var result = consumer.Requeue(message, delay);

        Assert.True(result);
        Assert.Equal(message, scheduler.Message);
        Assert.Equal(delay, scheduler.Delay);
    }

    private sealed class StubMessageScheduler : IAmAMessageSchedulerSync
    {
        public Message? Message { get; private set; }

        public TimeSpan? Delay { get; private set; }

        public string Schedule(Message message, DateTimeOffset at)
        {
            Message = message;
            Delay = at - DateTimeOffset.UtcNow;
            return "scheduled";
        }

        public string Schedule(Message message, TimeSpan delay)
        {
            Message = message;
            Delay = delay;
            return "scheduled";
        }

        public bool ReScheduler(string schedulerId, DateTimeOffset at)
        {
            return true;
        }

        public bool ReScheduler(string schedulerId, TimeSpan delay)
        {
            return true;
        }

        public void Cancel(string id)
        {
        }
    }
}
