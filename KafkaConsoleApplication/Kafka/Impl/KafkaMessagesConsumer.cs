using System.Collections.Concurrent;
using Confluent.Kafka;
using KafkaConsoleApplication.Kafka.Models;
using Polly;

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

            // consume batch messages based on count and timespan
            while (index++ < batchCount && DateTime.UtcNow - batchStartTime <= batchPeriod)
            {
                try
                {
                    // todo redis
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

            // concurrent bag for storing commitModel
            var partitionOffsetCommitModels = new ConcurrentBag<TopicPartitionOffsetCommitModel>();

            // local function adds commitModel to the bag if the processing (action from argument) was finished without any exceptions
            void ProcessMessagesByPartitionId(int partitionId)
            {
                try
                {
                    var messagesToProcess = messagesByPartition[partitionId]
                        .Select(result => result.Message)
                        .ToArray();

                    processMessages(messagesToProcess);

                    partitionOffsetCommitModels.Add(new TopicPartitionOffsetCommitModel
                    {
                        PartitionOffset = new TopicPartitionOffset(
                            messagesByPartition[partitionId].Last().TopicPartition,
                            messagesByPartition[partitionId].Last().Offset + 1),
                        StartOffset = messagesByPartition[partitionId].First().Offset.Value
                    });
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"Exception during batch processing. Exception message: {exception}");
                }
            }

            // process messages in parallel for each partition and fill out concurrent bag
            Parallel.ForEach(messagesByPartition.Keys, ProcessMessagesByPartitionId);

            // commit offsets, where batches were processed correctly without any exceptions
            await CommitOffsetsAsync(consumer, partitionOffsetCommitModels.ToArray(), cancellationToken);
        }
    }

    /// <summary>
    /// Commit offsets
    /// </summary>
    private async Task CommitOffsetsAsync<TKey, TValue>(IConsumer<TKey, TValue> consumer,
        TopicPartitionOffsetCommitModel[] offsetCommitModels, CancellationToken cancellationToken)
    {
        const int maxRetries = 3;
        const int retryPeriodSeconds = 2;

        // do commiting in parallel for each partition
        await Parallel.ForEachAsync(offsetCommitModels, cancellationToken, async (offsetCommitModel, token) =>
        {
            try
            {
                var retryPolicy = Policy.Handle<Exception>()
                    .WaitAndRetryAsync(retryCount: maxRetries,
                        sleepDurationProvider: _ => TimeSpan.FromSeconds(retryPeriodSeconds),
                        onRetry: (exception, sleepDuration, attemptNumber, context) =>
                        {
                            Console.WriteLine(
                                $"Retrying consumer committing in {sleepDuration}. {attemptNumber} / {maxRetries}");
                        });

                await retryPolicy.ExecuteAsync(() =>
                {
                    consumer.Commit(new[] { offsetCommitModel.PartitionOffset });
                    return Task.CompletedTask;
                });
            }
            // got something wrong during commiting offset
            // therefore we need to do "seek" to move to the old offset for our local consumer for the partition
            catch (Exception exception)
            {
                Console.WriteLine($"Fail after consumer commit retrying. Exception message {exception.Message} ");

                consumer.Seek(new TopicPartitionOffset(offsetCommitModel.PartitionOffset.TopicPartition,
                    offsetCommitModel.StartOffset));
            }
        });
    }
}