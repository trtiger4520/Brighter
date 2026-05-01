using System;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

namespace Paramore.Brighter.MessagingGateway.Nats;

internal static class NatsJetStreamProvisioner
{
    public static async Task<INatsJSContext> EnsureStreamAsync(
        NatsClient client,
        NatsMessagingGatewayConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var jetStream = client.CreateJetStreamContext();

        await jetStream.CreateOrUpdateStreamAsync(new StreamConfig
        {
            Name = configuration.StreamName,
            Subjects = [configuration.StreamSubjectPattern],
            Retention = StreamConfigRetention.Limits,
            Storage = ParseStorage(configuration.Storage),
            MaxAge = configuration.StreamMaxAge,
            MaxConsumers = -1,
            MaxMsgs = -1,
            MaxBytes = -1,
            DuplicateWindow = TimeSpan.FromMinutes(2)
        }, cancellationToken).ConfigureAwait(false);

        return jetStream;
    }

    public static ValueTask<INatsJSConsumer> EnsureConsumerAsync(
        INatsJSContext jetStream,
        NatsMessagingGatewayConsumerConfiguration configuration,
        string subject,
        string consumerName,
        CancellationToken cancellationToken)
    {
        return jetStream.GetConsumerAsync(configuration.StreamName, consumerName, cancellationToken);
    }

    public static string BuildConsumerName(string prefix, string subscriptionName)
    {
        var raw = string.IsNullOrWhiteSpace(prefix)
            ? subscriptionName
            : $"{prefix}-{subscriptionName}";

        Span<char> buffer = stackalloc char[raw.Length];
        var length = 0;

        foreach (var character in raw)
        {
            buffer[length++] = char.IsLetterOrDigit(character) || character is '-' or '_'
                ? char.ToLowerInvariant(character)
                : '-';
        }

        return new string(buffer[..length]).Trim('-');
    }

    private static StreamConfigStorage ParseStorage(string storage)
        => Enum.TryParse<StreamConfigStorage>(storage, ignoreCase: true, out var parsed)
            ? parsed
            : StreamConfigStorage.File;
}
