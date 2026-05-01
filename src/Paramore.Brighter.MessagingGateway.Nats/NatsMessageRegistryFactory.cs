using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsMessageRegistryFactory(
    NatsMessagingGatewayProducerConfiguration configuration,
    IEnumerable<NatsPublication> publications) : IAmAProducerRegistryFactory
{
    private readonly NatsMessagingGatewayProducerConfiguration _configuration = configuration;
    private readonly IEnumerable<NatsPublication> _publications = publications;

    public IAmAProducerRegistry Create()
    {
        NatsMessageProducerFactory producerFactory = new(_configuration, _publications);
        return new ProducerRegistry(producerFactory.Create());
    }

    public Task<IAmAProducerRegistry> CreateAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(Create());
    }
}
