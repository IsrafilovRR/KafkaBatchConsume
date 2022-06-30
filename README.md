# Kafka batch consume with exactly once processing

# Requirements
Реализовать батчевый kafka-консьюмер, который считывает N сообщений из топика и отдаёт их на обработку делегату обработчику void ProcessMessage<TKey, TValue>(IReadOnlyCollection<Message<TKey, TValue>> message) , который принимает на вход массив сообщений из топика.

важно предусмотреть:
- корректную обработку ошибок
- правильное сохранение оффсета в рамках партиции

задание со звёздочкой:
- реализовать exactly-once / идемпотентную обработку сообщений из топика с помощью редиса в качестве хранилища (отредактировано) 

## Init

1. Run docker container with kafka:
    `docker run -d -p 2181:2181 -p 9092:9092 -e ADVERTISED_HOST=127.0.0.1  -e NUM_PARTITIONS=10 johnnypark/kafka-zookeeper
    `
2. Run docker container with redis:
   `docker run -d -p 6379:6379 redis
   `
3. Generate messages:
    Undo commenting of `// await InitKafkaMessagesAsync(); ` row and run executor
