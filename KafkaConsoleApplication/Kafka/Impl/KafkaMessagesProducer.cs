using Confluent.Kafka;

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

        using var producer = new ProducerBuilder<string, string>(producerConfig)
            .Build();

        var rand = new Random();
        for (var i = 0; i < messagesCount; i++)
        {
            var msg = new Message<string, string>
            {
                Key = cityName,
                Value = i.ToString()
            };

            producer.Produce(topic, msg);
            await Task.Delay(10);
        }

        producer.Flush();
    }
}