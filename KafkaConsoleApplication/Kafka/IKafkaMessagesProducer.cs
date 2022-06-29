namespace KafkaConsoleApplication.Kafka;

/// <summary>
/// Message producer
/// </summary>
public interface IKafkaMessagesProducer
{
    /// <summary>
    /// Produce weather sensor messages
    /// </summary>
    Task ProduceTemperatureSensorMessages(string host, string topic, string cityName,
        int messagesCount = 1200);
}