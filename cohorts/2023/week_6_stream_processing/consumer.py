import argparse
from typing import Dict, List
from kafka import KafkaConsumer

from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
    "my-topic", group_id="my-group", bootstrap_servers=["localhost:9092"]
)
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print(
        "%s:%d:%d: key=%s value=%s"
        % (message.topic, message.partition, message.offset, message.key, message.value)
    )

conf = {
    "bootstrap.servers": "host1:9092,host2:9092",
    "group.id": "foo",
    "auto.offset.reset": "smallest",
}

consumer = Consumer(conf)


class RideCSVConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        print("Consuming from Kafka started")
        print("Available topics to consume: ", self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None or msg == {}:
                    continue
                for msg_key, msg_values in msg.items():
                    for msg_val in msg_values:
                        print(
                            f"Key:{msg_val.key}-type({type(msg_val.key)}), "
                            f"Value:{msg_val.value}-type({type(msg_val.value)})"
                        )
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer")
    parser.add_argument("--topic", type=str, default=CONSUME_TOPIC_RIDES_CSV)
    args = parser.parse_args()

    topic = args.topic
    config = {
        "bootstrap_servers": [BOOTSTRAP_SERVERS],
        "auto_offset_reset": "earliest",
        "enable_auto_commit": True,
        "key_deserializer": lambda key: int(key.decode("utf-8")),
        "value_deserializer": lambda value: value.decode("utf-8"),
        "group_id": "consumer.group.id.csv-example.1",
    }
    csv_consumer = RideCSVConsumer(props=config)
    csv_consumer.consume_from_kafka(topics=[topic])
