using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.JetStream;
using NATS.Net;

using Paramore.Brighter.JsonConverters;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessageConsumer : IAmAMessageConsumerSync, IAmAMessageConsumerAsync
{
    private readonly NatsMessagingGatewayConsumerConfiguration _configuration;
    private readonly ConcurrentQueue<Message> _messageQueue = new();
    private readonly ConcurrentDictionary<string, INatsJSMsg<string>> _pendingDeliveries = new();
    private readonly Message _noopMessage = new();
    private readonly string _subject;
    private readonly string _consumerName;
    private readonly RoutingKey? _deadLetterRoutingKey;
    private readonly RoutingKey? _invalidMessageRoutingKey;
    private readonly Lazy<NatsMessageProducer?>? _deadLetterProducer;
    private readonly Lazy<NatsMessageProducer?>? _invalidMessageProducer;
    private readonly NatsClient _natsClient;
    private readonly SemaphoreSlim _startLock = new(1, 1);
    private readonly CancellationTokenSource _subscriptionTokenSource = new();
    private readonly IAmAMessageScheduler? _scheduler;
    private INatsJSContext? _jetStream;
    private INatsJSConsumer? _consumer;
    private NatsMessageProducer? _requeueProducer;
    private bool _requeueProducerInitialized;
    private object? _requeueProducerLock;
    private bool _started;
    private Task? _listenTask;

    public NatsMessageConsumer(
        NatsMessagingGatewayConsumerConfiguration configuration,
        Subscription subscription,
        IAmAMessageScheduler? scheduler = null,
        RoutingKey? deadLetterRoutingKey = null,
        RoutingKey? invalidMessageRoutingKey = null)
    {
        _configuration = configuration;
        _scheduler = scheduler;
        _subject = NatsMessagePublisher.BuildSubject(subscription.RoutingKey, configuration.SubjectPrefix);
        _consumerName = NatsJetStreamProvisioner.BuildConsumerName(configuration.ConsumerNamePrefix, subscription.ChannelName.ToString());
        _deadLetterRoutingKey = deadLetterRoutingKey;
        _invalidMessageRoutingKey = invalidMessageRoutingKey;
        _natsClient = NatsMessagePublisher.CreateClient(configuration);

        if (_deadLetterRoutingKey != null)
        {
            _deadLetterProducer = new Lazy<NatsMessageProducer?>(CreateDeadLetterProducer, LazyThreadSafetyMode.None);
        }

        if (_invalidMessageRoutingKey != null)
        {
            _invalidMessageProducer = new Lazy<NatsMessageProducer?>(CreateInvalidMessageProducer, LazyThreadSafetyMode.None);
        }
    }

    public void Acknowledge(Message message)
    {
        AcknowledgeAsync(message).GetAwaiter().GetResult();
    }

    public async Task AcknowledgeAsync(Message message, CancellationToken cancellationToken = default)
    {
        if (!TryRemovePendingDelivery(message, out var pendingMessage))
        {
            return;
        }

        await pendingMessage.AckAsync(null, cancellationToken).ConfigureAwait(false);
    }

    public void Nack(Message message)
    {
        NackAsync(message).GetAwaiter().GetResult();
    }

    public async Task NackAsync(Message message, CancellationToken cancellationToken = default)
    {
        if (!TryRemovePendingDelivery(message, out var pendingMessage))
        {
            return;
        }

        await pendingMessage.NakAsync(null, cancellationToken).ConfigureAwait(false);
    }

    public void Purge()
    {
        _messageQueue.Clear();
        _pendingDeliveries.Clear();
    }

    public Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        Purge();
        return Task.CompletedTask;
    }

    public Message[] Receive(TimeSpan? timeOut = null)
    {
        EnsureStarted();

        if (_messageQueue.IsEmpty)
        {
            return [_noopMessage];
        }

        var messages = new List<Message>();
        while (_messageQueue.TryDequeue(out var message))
        {
            messages.Add(message);
        }

        return messages.Count == 0 ? [_noopMessage] : messages.ToArray();
    }

    public Task<Message[]> ReceiveAsync(TimeSpan? timeOut = null, CancellationToken cancellationToken = default)
    {
        return EnsureStartedAsync(cancellationToken).ContinueWith(_ => Receive(timeOut), cancellationToken);
    }

    public bool Reject(Message message, MessageRejectionReason? reason = null)
    {
        var (producer, routingKey) = ResolveRejectionProducer(message, reason);
        if (producer == null || routingKey == null)
        {
            Acknowledge(message);
            return true;
        }

        try
        {
            producer.Send(message);
            Acknowledge(message);
            return true;
        }
        catch
        {
            return true;
        }
    }

    public async Task<bool> RejectAsync(Message message, MessageRejectionReason? reason = null, CancellationToken cancellationToken = default)
    {
        var (producer, routingKey) = ResolveRejectionProducer(message, reason);
        if (producer == null || routingKey == null)
        {
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            return true;
        }

        try
        {
            await producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return true;
        }
    }

    public bool Requeue(Message message, TimeSpan? delay = null)
    {
        delay ??= TimeSpan.Zero;
        EnsureRequeueProducer();

        if (delay > TimeSpan.Zero)
        {
            _requeueProducer!.SendWithDelay(message, delay);
            Acknowledge(message);
            return true;
        }

        _requeueProducer!.Send(message);
        Acknowledge(message);
        return true;
    }

    public async Task<bool> RequeueAsync(Message message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
    {
        delay ??= TimeSpan.Zero;
        EnsureRequeueProducer();

        if (delay > TimeSpan.Zero)
        {
            await _requeueProducer!.SendWithDelayAsync(message, delay, cancellationToken).ConfigureAwait(false);
            await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
            return true;
        }

        await _requeueProducer!.SendAsync(message, cancellationToken).ConfigureAwait(false);
        await AcknowledgeAsync(message, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private (NatsMessageProducer? producer, RoutingKey? routingKey) ResolveRejectionProducer(Message message, MessageRejectionReason? reason)
    {
        if (_deadLetterProducer == null && _invalidMessageProducer == null)
        {
            return (null, null);
        }

        RefreshMetadata(message, reason);

        var (routingKey, hasProducer, _) = DetermineRejectionRoute(reason);
        if (!hasProducer)
        {
            return (null, null);
        }

        var producer = routingKey == _invalidMessageRoutingKey
            ? _invalidMessageProducer?.Value
            : _deadLetterProducer?.Value;

        return (producer, routingKey);
    }

    private void EnsureStarted()
    {
        EnsureStartedAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    private async Task EnsureStartedAsync(CancellationToken cancellationToken)
    {
        if (_started)
        {
            return;
        }

        await _startLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_started)
            {
                return;
            }

            await _natsClient.ConnectAsync().ConfigureAwait(false);
            _jetStream = _natsClient.CreateJetStreamContext();
            _consumer = await NatsJetStreamProvisioner.EnsureConsumerAsync(_jetStream, _configuration, _subject, _consumerName, cancellationToken).ConfigureAwait(false);
            _listenTask = Task.Run(() => ListenAsync(_subscriptionTokenSource.Token), CancellationToken.None);
            _started = true;
        }
        finally
        {
            _startLock.Release();
        }
    }

    private async Task ListenAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var natsMessage = await _consumer!.NextAsync<string>(
                    serializer: null,
                    opts: new NatsJSNextOpts { Expires = _configuration.FetchTimeout },
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                if (natsMessage?.Data == null)
                {
                    continue;
                }

                var message = JsonSerializer.Deserialize<Message>(natsMessage.Data, JsonSerialisationOptions.Options);
                if (message != null)
                {
                    _pendingDeliveries[GetMessageKey(message)] = natsMessage;
                    _messageQueue.Enqueue(message);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void EnsureRequeueProducer()
    {
        LazyInitializer.EnsureInitialized(ref _requeueProducer, ref _requeueProducerInitialized, ref _requeueProducerLock, () =>
        {
            var publisher = new NatsMessagePublisher(new NatsMessagingGatewayProducerConfiguration
            {
                Url = _configuration.Url,
                SubjectPrefix = _configuration.SubjectPrefix,
                ConnectionName = _configuration.ConnectionName,
                StreamName = _configuration.StreamName,
                StreamSubjectPattern = _configuration.StreamSubjectPattern,
                ConsumerNamePrefix = _configuration.ConsumerNamePrefix,
                AckWait = _configuration.AckWait,
                MaxDeliver = _configuration.MaxDeliver,
                MaxAckPending = _configuration.MaxAckPending,
                FetchTimeout = _configuration.FetchTimeout,
                StreamMaxAge = _configuration.StreamMaxAge,
                Storage = _configuration.Storage,
                ConnectionAttempts = _configuration.ConnectionAttempts,
                ReconnectWait = _configuration.ReconnectWait
            });

            return new NatsMessageProducer(publisher, new Publication())
            {
                Scheduler = _scheduler
            };
        });
    }

    private static void RefreshMetadata(Message message, MessageRejectionReason? reason)
    {
        message.Header.Bag["originalTopic"] = message.Header.Topic.Value;
        message.Header.Bag["rejectionTimestamp"] = DateTimeOffset.UtcNow.ToString("o");
        message.Header.Bag["originalMessageType"] = message.Header.MessageType.ToString();

        if (reason == null)
        {
            return;
        }

        message.Header.Bag["rejectionReason"] = reason.RejectionReason.ToString();
        if (!string.IsNullOrEmpty(reason.Description))
        {
            message.Header.Bag["rejectionMessage"] = reason.Description ?? string.Empty;
        }
    }

    private (RoutingKey? routingKey, bool hasProducer, bool isFallingBackToDlq) DetermineRejectionRoute(MessageRejectionReason? reason)
    {
        var hasInvalidProducer = _invalidMessageProducer != null;
        var hasDeadLetterProducer = _deadLetterProducer != null;

        return reason?.RejectionReason switch
        {
            RejectionReason.Unacceptable when hasInvalidProducer => (_invalidMessageRoutingKey, true, false),
            RejectionReason.Unacceptable when hasDeadLetterProducer => (_deadLetterRoutingKey, true, true),
            RejectionReason.DeliveryError when hasDeadLetterProducer => (_deadLetterRoutingKey, true, false),
            _ when hasDeadLetterProducer => (_deadLetterRoutingKey, true, false),
            _ => (null, false, false)
        };
    }

    private NatsMessageProducer? CreateDeadLetterProducer()
    {
        if (_deadLetterRoutingKey == null)
        {
            return null;
        }

        var publisher = new NatsMessagePublisher(new NatsMessagingGatewayProducerConfiguration
        {
            Url = _configuration.Url,
            SubjectPrefix = _configuration.SubjectPrefix,
            ConnectionName = string.IsNullOrWhiteSpace(_configuration.ConnectionName)
                ? null
                : $"{_configuration.ConnectionName}-dlq",
            StreamName = _configuration.StreamName,
            StreamSubjectPattern = _configuration.StreamSubjectPattern,
            ConsumerNamePrefix = _configuration.ConsumerNamePrefix,
            AckWait = _configuration.AckWait,
            MaxDeliver = _configuration.MaxDeliver,
            MaxAckPending = _configuration.MaxAckPending,
            FetchTimeout = _configuration.FetchTimeout,
            StreamMaxAge = _configuration.StreamMaxAge,
            Storage = _configuration.Storage,
            ConnectionAttempts = _configuration.ConnectionAttempts,
            ReconnectWait = _configuration.ReconnectWait
        });

        return new NatsMessageProducer(publisher, new Publication { Topic = _deadLetterRoutingKey });
    }

    private NatsMessageProducer? CreateInvalidMessageProducer()
    {
        if (_invalidMessageRoutingKey == null)
        {
            return null;
        }

        var publisher = new NatsMessagePublisher(new NatsMessagingGatewayProducerConfiguration
        {
            Url = _configuration.Url,
            SubjectPrefix = _configuration.SubjectPrefix,
            ConnectionName = string.IsNullOrWhiteSpace(_configuration.ConnectionName)
                ? null
                : $"{_configuration.ConnectionName}-invalid",
            StreamName = _configuration.StreamName,
            StreamSubjectPattern = _configuration.StreamSubjectPattern,
            ConsumerNamePrefix = _configuration.ConsumerNamePrefix,
            AckWait = _configuration.AckWait,
            MaxDeliver = _configuration.MaxDeliver,
            MaxAckPending = _configuration.MaxAckPending,
            FetchTimeout = _configuration.FetchTimeout,
            StreamMaxAge = _configuration.StreamMaxAge,
            Storage = _configuration.Storage,
            ConnectionAttempts = _configuration.ConnectionAttempts,
            ReconnectWait = _configuration.ReconnectWait
        });

        return new NatsMessageProducer(publisher, new Publication { Topic = _invalidMessageRoutingKey });
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        _subscriptionTokenSource.Cancel();

        if (_listenTask != null)
        {
            try
            {
                await _listenTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (_requeueProducer != null)
        {
            await _requeueProducer.DisposeAsync().ConfigureAwait(false);
        }

        if (_deadLetterProducer is { IsValueCreated: true } && _deadLetterProducer.Value != null)
        {
            await _deadLetterProducer.Value.DisposeAsync().ConfigureAwait(false);
        }

        if (_invalidMessageProducer is { IsValueCreated: true } && _invalidMessageProducer.Value != null)
        {
            await _invalidMessageProducer.Value.DisposeAsync().ConfigureAwait(false);
        }

        await _natsClient.DisposeAsync().ConfigureAwait(false);
        _subscriptionTokenSource.Dispose();
        _startLock.Dispose();
        GC.SuppressFinalize(this);
    }

    private static string GetMessageKey(Message message)
        => message.Id.ToString();

    private bool TryRemovePendingDelivery(Message message, out INatsJSMsg<string> pendingMessage)
        => _pendingDeliveries.TryRemove(GetMessageKey(message), out pendingMessage!);

}
