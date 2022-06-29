using Confluent.Kafka;

namespace KafkaConsoleApplication.Kafka.Models;

/// <summary>
/// Offset commit model
/// </summary>
internal record TopicPartitionOffsetCommitModel
{
    /// <summary>
    /// Partition offset
    /// </summary>
    public TopicPartitionOffset PartitionOffset { get; init; }

    /// <summary>
    /// Start offset
    /// </summary>
    /// <remarks>\
    /// This field needs when we have to do seek if offset commit was failed
    /// </remarks>
    public long StartOffset { get; init; }
}