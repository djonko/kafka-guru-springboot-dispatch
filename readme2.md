# How to start Kafka server
### Download kafka

### setup
- set KAFKA_CLUSTER_ID env var
```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```
- Format log directory location
```bash
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

### Run a producer in cli

```bash
bin/kafka-console-producer.sh --topic order.created --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=:
```

### Run a consumer in cli
```bash
bin/kafka-console-consumer.sh --topic order.dispatched --bootstrap-server localhost:9092 --property print.key=true --property key.separator=:
```

### describe topic command while the server is running
```bash
 bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic name.of.topic
```
- modify the number of partition in a topic
```bash
 bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic name.of.topic --partitions 5
```