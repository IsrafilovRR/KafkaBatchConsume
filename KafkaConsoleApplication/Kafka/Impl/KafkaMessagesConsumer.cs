using System.Collections.Concurrent;
using Confluent.Kafka;

namespace KafkaConsoleApplication.Kafka.Impl;

/// <inheritdoc />
public class KafkaMessagesConsumer : IKafkaMessagesConsumer
{
    /// <inheritdoc />
    public async Task ConsumeMessagesAsync<TKey, TValue>(string host, string topic, string groupId,
        Action<Message<TKey, TValue>> processMessage,
        CancellationToken cancellationToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = host,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin
        };

        var consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
            .Build();

        consumer.Subscribe(new[] { topic });

        while (!cancellationToken.IsCancellationRequested)
        {
            var msg = consumer.Consume(TimeSpan.FromMilliseconds(100));

            try
            {
                if (msg != null)
                {
                    processMessage(msg.Message);
                    //consumer.Commit(msg);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"processing error ({msg.Topic}:{msg.Partition}:{msg.Offset}): {e.Message}");
            }

            // simulate 
            await Task.Delay(TimeSpan.FromMilliseconds(300), cancellationToken);
        }
    }

    /// <inheritdoc />
    public async Task BatchConsumeMessagesAsync<TKey, TValue>(string host, string topic, string groupId,
        Action<IReadOnlyCollection<Message<TKey, TValue>>> processMessages,
        CancellationToken cancellationToken,
        TimeSpan? batchPeriod, int batchCount)
    {
        batchPeriod ??= TimeSpan.FromMinutes(1);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = host,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // we will commit manually
            EnableAutoCommit = false
        };

        var consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
            .Build();

        consumer.Subscribe(new[] { topic });

        while (!cancellationToken.IsCancellationRequested)
        {
            var consumedMessages = new List<ConsumeResult<TKey, TValue>>(batchCount);
            var batchStartTime = DateTime.UtcNow;
            var index = 0;

            // consume batch messages
            while (index++ < batchCount && DateTime.UtcNow - batchStartTime <= batchPeriod)
            {
                try
                {
                    var message = consumer.Consume(batchPeriod.Value);
                    if (message is { Message: { } })
                    {
                        consumedMessages.Add(message);
                    }
                }
                catch (ConsumeException exception)
                {
                    // need to decrement index since catch consume exception
                    index -= 1;
                    Console.WriteLine($"Consuming error: ({exception.Message}");
                }
                catch (OperationCanceledException exception)
                {
                    Console.WriteLine($"Got cancellation: ({exception.Message}");
                    throw;
                }
            }

            if (consumedMessages.Count == 0) continue;

            // create pairs: partition id -> ordered messages by offset
            var messagesByPartition = consumedMessages
                .GroupBy(result => result.Partition.Value)
                .ToDictionary(grouping => grouping.Key,
                    result => result
                        .OrderBy(consumeResult => consumeResult.Offset.Value)
                        .ToArray());

            var offsetsToCommitBag = new ConcurrentBag<TopicPartitionOffset>();

            void ProcessMessagesByPartitionId(int partitionId)
            {
                try
                {
                    var messagesToProcess = messagesByPartition[partitionId]
                        .Select(result => result.Message)
                        .ToArray();

                    processMessages(messagesToProcess);

                    offsetsToCommitBag.Add(new TopicPartitionOffset(
                        messagesByPartition[partitionId].Last().TopicPartition,
                        messagesByPartition[partitionId].Last().Offset + 1));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"Exception during batch processing. Exception message: {exception}");
                }
            }

            Parallel.ForEach(messagesByPartition.Keys, ProcessMessagesByPartitionId);

            // commit only offsets, which batches were processed correctly
            consumer.Commit(offsetsToCommitBag.ToArray());
        }
    }
}