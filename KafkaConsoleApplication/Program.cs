using KafkaConsoleApplication.Kafka;
using KafkaConsoleApplication.Kafka.Impl;
using KafkaConsoleApplication.Redis;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaConsoleApplication;

public static class Program
{
    static void Main(string[] args)
    {
        var services = new ServiceCollection();
        ConfigureServices(services);
        services
            .AddSingleton<Executor, Executor>()
            .BuildServiceProvider()
            .GetService<Executor>()
            ?.Execute().Wait();
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        services
            .AddSingleton<IKafkaMessagesConsumer, KafkaMessagesConsumer>()
            .AddSingleton<IKafkaMessagesProducer, KafkaMessagesProducer>()
            .AddSingleton<IRedisRepository, RedisRepository>();
    }
}