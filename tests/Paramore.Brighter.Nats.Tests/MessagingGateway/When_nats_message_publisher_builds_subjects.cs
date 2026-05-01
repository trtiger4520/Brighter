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

using Paramore.Brighter.MessagingGateway.Nats;
using Xunit;

namespace Paramore.Brighter.Nats.Tests.MessagingGateway;

public class When_nats_message_publisher_builds_subjects
{
    [Fact]
    public void Should_prepend_subject_prefix_when_present()
    {
        var subject = NatsMessagePublisher.BuildSubject(new RoutingKey("orders.created"), "brighter");

        Assert.Equal("brighter.orders.created", subject);
    }

    [Fact]
    public void Should_return_the_routing_key_when_no_prefix_is_present()
    {
        var subject = NatsMessagePublisher.BuildSubject(new RoutingKey("orders.created"), null);

        Assert.Equal("orders.created", subject);
    }
}
