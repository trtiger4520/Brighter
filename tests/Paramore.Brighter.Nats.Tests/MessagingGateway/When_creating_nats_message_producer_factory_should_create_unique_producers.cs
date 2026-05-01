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
using Paramore.Brighter.Nats.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.Nats.Tests.MessagingGateway;

public class When_creating_nats_message_producer_factory_should_create_unique_producers
{
    [Fact]
    public void Should_create_a_producer_for_each_publication()
    {
        var configuration = new NatsMessagingGatewayProducerConfiguration
        {
            Url = "nats://localhost:4222",
            SubjectPrefix = "brighter"
        };

        var firstPublication = new NatsPublication<MyCommand>
        {
            Topic = new RoutingKey("orders.created"),
            Type = new CloudEventsType("tests.orders.created")
        };

        var secondPublication = new NatsPublication<MyCommand>
        {
            Topic = new RoutingKey("orders.cancelled"),
            Type = new CloudEventsType("tests.orders.cancelled")
        };

        var factory = new NatsMessageProducerFactory(configuration, [firstPublication, secondPublication]);

        var producers = factory.Create();

        Assert.Equal(2, producers.Count);
        Assert.Contains(new ProducerKey(firstPublication.Topic!, firstPublication.Type), producers.Keys);
        Assert.Contains(new ProducerKey(secondPublication.Topic!, secondPublication.Type), producers.Keys);
    }

    [Fact]
    public void Should_throw_if_two_publications_share_the_same_topic_and_type()
    {
        var configuration = new NatsMessagingGatewayProducerConfiguration
        {
            Url = "nats://localhost:4222"
        };

        var publication = new NatsPublication<MyCommand>
        {
            Topic = new RoutingKey("orders.created"),
            Type = new CloudEventsType("tests.orders.created")
        };

        var factory = new NatsMessageProducerFactory(configuration, [publication, publication]);

        Assert.Throws<ArgumentException>(() => factory.Create());
    }
}
