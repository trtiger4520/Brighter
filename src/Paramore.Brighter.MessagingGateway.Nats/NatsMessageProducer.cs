using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.Tasks;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessageProducer(
    NatsMessagePublisher publisher,
    Publication publication) : IAmAMessageProducerAsync, IAmAMessageProducerSync
{
    private readonly NatsMessagePublisher _publisher = publisher;

    public int MaxOutStandingMessages { get; set; } = -1;

    public int MaxOutStandingCheckIntervalMilliSeconds { get; set; }

    public Dictionary<string, object> OutBoxBag { get; set; } = [];

    public Publication Publication { get; set; } = publication;

    public Activity? Span { get; set; }

    public IAmAMessageScheduler? Scheduler { get; set; }

    public void Dispose()
    {
        _publisher.Dispose();
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await _publisher.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    public void Send(Message message)
    {
        SendWithDelay(message, TimeSpan.Zero);
    }

    public Task SendAsync(Message message, CancellationToken cancellationToken = default)
    {
        return SendWithDelayAsync(message, TimeSpan.Zero, cancellationToken);
    }

    public void SendWithDelay(Message message, TimeSpan? delay = null)
    {
        delay ??= TimeSpan.Zero;
        if (delay != TimeSpan.Zero)
        {
            if (Scheduler is IAmAMessageSchedulerSync sync)
            {
                sync.Schedule(message, delay.Value);
                return;
            }

            if (Scheduler is IAmAMessageSchedulerAsync asyncScheduler)
            {
                BrighterAsyncContext.Run(() => asyncScheduler.ScheduleAsync(message, delay.Value));
                return;
            }

            throw new ConfigurationException(
                $"NatsMessageProducer: delay of {delay} was requested but no scheduler is configured; configure a scheduler via MessageSchedulerFactory.");
        }

        _publisher.PublishMessage(message, Publication.Topic);
    }

    public async Task SendWithDelayAsync(Message message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
    {
        delay ??= TimeSpan.Zero;
        if (delay != TimeSpan.Zero)
        {
            if (Scheduler is IAmAMessageSchedulerAsync asyncScheduler)
            {
                await asyncScheduler.ScheduleAsync(message, delay.Value, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (Scheduler is IAmAMessageSchedulerSync sync)
            {
                sync.Schedule(message, delay.Value);
                return;
            }

            throw new ConfigurationException(
                $"NatsMessageProducer: delay of {delay} was requested but no scheduler is configured; configure a scheduler via MessageSchedulerFactory.");
        }

        await _publisher.PublishMessageAsync(message, Publication.Topic, cancellationToken).ConfigureAwait(false);
    }
}
