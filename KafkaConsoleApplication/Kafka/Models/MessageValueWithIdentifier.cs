namespace KafkaConsoleApplication.Kafka.Models;

/// <summary>
/// Kafka message value with identifier
/// </summary>
public abstract class MessageValueWithIdentifier
{
    public Guid Id { get; init; }
}