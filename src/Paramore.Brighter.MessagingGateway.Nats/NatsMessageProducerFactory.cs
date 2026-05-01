using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessageProducerFactory(
    NatsMessagingGatewayProducerConfiguration configuration,
    IEnumerable<NatsPublication> publications) : IAmAMessageProducerFactory
{
    private readonly NatsMessagingGatewayProducerConfiguration _configuration = configuration;
    private readonly IEnumerable<NatsPublication> _publications = publications;

    public Dictionary<ProducerKey, IAmAMessageProducer> Create()
    {
        var producers = new Dictionary<ProducerKey, IAmAMessageProducer>();

        foreach (var publication in _publications)
        {
            if (publication.Topic is null)
            {
                throw new ConfigurationException("NatsMessageProducerFactory.Create => A NatsPublication must have a topic/routing key");
            }

            var producer = new NatsMessageProducer(new NatsMessagePublisher(_configuration), publication)
            {
                Publication = publication
            };

            var producerKey = new ProducerKey(publication.Topic, publication.Type);
            if (producers.ContainsKey(producerKey))
            {
                throw new ArgumentException($"A publication with the topic {publication.Topic} and {publication.Type} already exists in the producer registry. Each topic + type must be unique in the producer registry.");
            }

            producers[producerKey] = producer;
        }

        return producers;
    }

    public Task<Dictionary<ProducerKey, IAmAMessageProducer>> CreateAsync()
    {
        return Task.FromResult(Create());
    }
}
