using KafkaConsoleApplication.Kafka;

namespace KafkaConsoleApplication;

public class Executor
{
    private readonly IKafkaMessagesConsumer _messagesConsumer;
    private readonly IKafkaMessagesProducer _messagesProducer;

    private const string KafkaHost = "localhost:9092";
    private const string TemperatureSensorMessagesTopic = "temperature_sensor";
    private const string GroupId = "group_id_1";

    public Executor(IKafkaMessagesConsumer messagesConsumer, IKafkaMessagesProducer messagesProducer)
    {
        _messagesConsumer = messagesConsumer;
        _messagesProducer = messagesProducer;
    }

    public async Task Execute()
    {
        try
        {
            // await InitKafkaMessagesAsync();

            await _messagesConsumer.BatchConsumeMessagesAsync<string, string>(KafkaHost,
                TemperatureSensorMessagesTopic, GroupId,
                (messages) =>
                {
                    Console.WriteLine($"Consumed messages count - {messages.Count}");
            
                    foreach (var message in messages)
                    {
                        Console.WriteLine($"Key - {message.Key}, value - {message.Value} ");
                    }
            
                    Console.WriteLine($"Thread id: {Thread.CurrentThread.ManagedThreadId}");
                    Thread.Sleep(300);
                },
                CancellationToken.None);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

        Console.ReadLine();
    }

    private Task InitKafkaMessagesAsync()
    {
        var createKazanMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Kazan");

        var createMoscowMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Moscow");

        var createUfaMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Ufa");

        var createOmskMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Omsk");

        var createKorolevMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Korolev");

        var createSaratovMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Saratov");

        var createAlmetyevskMessagesTask = _messagesProducer.ProduceTemperatureSensorMessages(KafkaHost,
            TemperatureSensorMessagesTopic,
            "Almetyevsk");

        return Task.WhenAll(new[]
        {
            createKazanMessagesTask, createMoscowMessagesTask,
            createUfaMessagesTask, createKorolevMessagesTask, createOmskMessagesTask, createSaratovMessagesTask,
            createAlmetyevskMessagesTask
        });
    }
}