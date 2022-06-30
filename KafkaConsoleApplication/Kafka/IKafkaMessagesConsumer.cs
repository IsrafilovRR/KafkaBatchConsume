using Confluent.Kafka;
using KafkaConsoleApplication.Kafka.Models;

namespace KafkaConsoleApplication.Kafka;

/// <summary>
///  Kafka consumer
/// </summary>
public interface IKafkaMessagesConsumer
{
    /// <summary>
    /// Consume messages one by one
    /// </summary>
    Task ConsumeMessagesAsync<TKey, TValue>(string host, string topic, string groupId,
        Action<Message<TKey, TValue>> processMessage,
        CancellationToken cancellationToken);
    
    /// <summary>
    /// Consume messages in batch
    /// </summary>
    Task BatchConsumeMessagesAsync<TKey, TValue>(string host, string topic, string groupId,
        Action<IReadOnlyCollection<Message<TKey, TValue>>> processMessages,
        CancellationToken cancellationToken,
        TimeSpan? batchPeriod = default, int batchCount = 2500) where TValue : MessageValueWithIdentifier;
}