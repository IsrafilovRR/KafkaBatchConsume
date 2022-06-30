namespace KafkaConsoleApplication.Kafka.Models;

/// <summary>
/// Message value for testing purposes
/// </summary>
public class TestMessageValue : MessageValueWithIdentifier
{
    public int SomeValue { get; init; }
}