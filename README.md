# Homework 5. Kafka batch consume with exactly once processing

## Init

1. Run docker container with kafka:
    `docker run -d -p 2181:2181 -p 9092:9092 -e ADVERTISED_HOST=127.0.0.1  -e NUM_PARTITIONS=10 johnnypark/kafka-zookeeper
    `
2. Run docker container with redis:
   `docker run -d -p 6379:6379 redis
   `
3. Generate messages:
    Undo commenting of `// await InitKafkaMessagesAsync(); ` row and run executor