using System;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessagingGatewayConfiguration : IAmGatewayConfiguration
{
    public string Url { get; set; } = "nats://127.0.0.1:4222";

    public string? SubjectPrefix { get; set; }

    public string? QueueGroup { get; set; }

    public string? ConnectionName { get; set; }

    public string StreamName { get; set; } = "learn-brighter";

    public string StreamSubjectPattern { get; set; } = "learn-brighter.>";

    public string ConsumerNamePrefix { get; set; } = "learn-brighter-worker";

    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxDeliver { get; set; } = 5;

    public int MaxAckPending { get; set; } = 64;

    public TimeSpan FetchTimeout { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan StreamMaxAge { get; set; } = TimeSpan.FromDays(7);

    public string Storage { get; set; } = "File";

    public int ConnectionAttempts { get; set; } = 1;

    public TimeSpan ReconnectWait { get; set; } = TimeSpan.FromSeconds(2);
}

public class NatsMessagingGatewayProducerConfiguration : NatsMessagingGatewayConfiguration;

public class NatsMessagingGatewayConsumerConfiguration : NatsMessagingGatewayConfiguration;
