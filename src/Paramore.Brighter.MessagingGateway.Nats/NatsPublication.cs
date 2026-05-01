namespace Paramore.Brighter.MessagingGateway.Nats;

public class NatsPublication : Publication;

public class NatsPublication<T> : NatsPublication where T : class, IRequest
{
    public NatsPublication()
    {
        RequestType = typeof(T);
    }
}