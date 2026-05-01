using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Core;
using NATS.Net;

using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.Tasks;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessagePublisher(
    NatsMessagingGatewayConfiguration config) : IDisposable, IAsyncDisposable
{
    private readonly NatsMessagingGatewayConfiguration _config = config;
    private readonly NatsClient _natsClient = CreateClient(config);
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private bool _connected;
    private bool _disposed;

    public static string BuildSubject(Message message, string? subjectPrefix)
    {
        ArgumentNullException.ThrowIfNull(message);
        return BuildSubject(message.Header.Topic, subjectPrefix);
    }

    public static string BuildSubject(RoutingKey routingKey, string? subjectPrefix)
    {
        ArgumentNullException.ThrowIfNull(routingKey);

        if (string.IsNullOrWhiteSpace(subjectPrefix))
        {
            return routingKey.Value;
        }

        if (string.IsNullOrWhiteSpace(routingKey.Value))
        {
            return subjectPrefix;
        }

        return $"{subjectPrefix}.{routingKey.Value}";
    }

    public void PublishMessage(Message message, RoutingKey? routingKey = null)
    {
        BrighterAsyncContext.Run(() => PublishMessageAsync(message, routingKey));
    }

    public async Task PublishMessageAsync(Message message, RoutingKey? routingKey = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

        var subject = BuildSubject(routingKey ?? message.Header.Topic, _config.SubjectPrefix);
        var payload = JsonSerializer.Serialize(message, JsonSerialisationOptions.Options);

        await _natsClient.PublishAsync(subject, payload, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    internal static NatsClient CreateClient(NatsMessagingGatewayConfiguration config)
    {
        var options = NatsOpts.Default with
        {
            Url = config.Url,
            Name = string.IsNullOrWhiteSpace(config.ConnectionName) ? "Paramore.Brighter.Nats" : config.ConnectionName,
            RetryOnInitialConnect = false,
            ReconnectWaitMin = config.ReconnectWait,
            ReconnectWaitMax = config.ReconnectWait,
            MaxReconnectRetry = Math.Max(config.ConnectionAttempts - 1, 0)
        };

        return new NatsClient(options);
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_connected)
        {
            return;
        }

        await _connectLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connected)
            {
                return;
            }

            var attempts = Math.Max(1, _config.ConnectionAttempts);
            Exception? lastException = null;

            for (var attempt = 0; attempt < attempts; attempt++)
            {
                try
                {
                    await _natsClient.ConnectAsync().ConfigureAwait(false);
                    _connected = true;
                    return;
                }
                catch (Exception ex) when (attempt < attempts - 1)
                {
                    lastException = ex;
                    await Task.Delay(_config.ReconnectWait, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    break;
                }
            }

            throw lastException ?? new InvalidOperationException("Unable to connect to the configured NATS server.");
        }
        finally
        {
            _connectLock.Release();
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _natsClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _connectLock.Dispose();
        }

        _disposed = true;
    }

    protected virtual async ValueTask DisposeAsyncCore()
    {
        if (_disposed)
        {
            return;
        }

        await _natsClient.DisposeAsync().ConfigureAwait(false);
        _connectLock.Dispose();
        _disposed = true;
    }
}
