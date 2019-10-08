from kafka import KafkaConsumer
from typing import Set

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from film_record import FilmRecord
from logger import CustomLogger

# Global presets
file_name = "C:\\Users\\avikulin\\Desktop\\movies.json"
kafka_brockers = ["10.40.1.142:9092", "10.40.1.141:9092"]
kafka_topic_name = "avikulin_test"
kafka_clientid = "Python test util"
kafka_value_serializer = FilmRecord.deserialize

if __name__ == "__main__":
    logger_instance = CustomLogger("kafka_read_util")
    logger_instance.activate()

    kafka_consumer_group_id = input("Apache Kafka consumer group id:\t")

    consumer = KafkaConsumer(kafka_topic_name,
                             group_id=kafka_consumer_group_id,
                             bootstrap_servers=kafka_brockers,
                             enable_auto_commit=False,
                             client_id=kafka_clientid)

    kafka_partitions: Set[TopicPartition] = consumer.partitions_for_topic(kafka_topic_name)
    # kafka_partitions_assigned: Set[TopicPartition] = consumer.assignment()

    # msg: KafkaMessage
    for msg in consumer:
        topic_positions: str = ""
        for p in kafka_partitions:
            topic_positions += f"[part.# {p} - {consumer.position(p)} " \
                f"(min. = {consumer.beginning_offsets(p)} / max. = {consumer.end_offsets(p)})] "

        print(f"partition - {msg.partition}")
        print(f"offset - {topic_positions}")
        print(f"key - {msg.key.decode('utf-8')}")
        print(f"value - {msg.value.decode('utf-8')}")

        command = input("Command: Next (press \"Enter\"), Commit (C), Seek (S), Quit (Q)\t")
        if command == "Q":
            break
        elif command == "C":
            consumer.commit()
        elif command == "S":
            pass
