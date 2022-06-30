using Confluent.Kafka;
using KafkaConsoleApplication.Kafka.Models;
using KafkaConsoleApplication.Kafka.Serializers;

namespace KafkaConsoleApplication.Kafka.Impl;

/// <inheritdoc />
public class KafkaMessagesProducer : IKafkaMessagesProducer
{
    /// <inheritdoc />
    public async Task ProduceTemperatureSensorMessages(string host, string topic, string cityName,
        int messagesCount = 500)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = host
        };

        using var producer = new ProducerBuilder<string, TestMessageValue>(producerConfig)
            .SetValueSerializer(new CustomKafkaSerializer<TestMessageValue>())
            .Build();

        for (var i = 0; i < messagesCount; i++)
        {
            var msg = new Message<string, TestMessageValue>
            {
                Key = cityName,
                Value = new TestMessageValue
                {
                    Id = Guid.NewGuid(),
                    SomeValue = i
                }
            };

            producer.Produce(topic, msg);
            await Task.Delay(10);
        }

        producer.Flush();
    }
}