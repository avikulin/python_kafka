from kafka import KafkaConsumer
from typing import Set, List, Tuple
from kafka.structs import TopicPartition, KafkaMessage

from logger import CustomLogger, LogLevels

from sys import exit

# Global presets
kafka_brokers = ["10.40.1.142:9092", "10.40.1.141:9092"]
kafka_topic_name = "avikulin_test"
kafka_consumer_group_id = "test_group#111"
kafka_client_id = __file__

if __name__ == "__main__":
    # Enable logging for kafka consumer
    kafka_logger = CustomLogger("kafka", log_level= LogLevels.DEBUG)
    kafka_logger.activate()

    # Enable logging for app
    app_logger = CustomLogger("kafka_read_util", log_level=LogLevels.INFO)
    app_logger.activate()

    while True:
        consumer = KafkaConsumer(
            kafka_topic_name,
            group_id=kafka_consumer_group_id,
            client_id=kafka_client_id,
            bootstrap_servers=kafka_brokers,
            request_timeout_ms=6001,
            session_timeout_ms=6000,
            heartbeat_interval_ms=2000,
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )
        app_logger.get.info("Consumer init successful")

        kafka_partitions: Set[TopicPartition] = consumer.partitions_for_topic(kafka_topic_name)
        kafka_topic_partitions: List[TopicPartition] = [TopicPartition(kafka_topic_name, p) for p in kafka_partitions]
        kafka_min_offsets = consumer.beginning_offsets(kafka_topic_partitions)
        kafka_max_offsets = consumer.end_offsets(kafka_topic_partitions)

        app_logger.get.info(f"Cluster info: brokers - {kafka_brokers} partitions - {kafka_topic_partitions} "
                                 f"min. offset - {kafka_min_offsets} "
                                 f"max. offsets - {kafka_max_offsets}")

        # Print connection statistics
        print("\nClaster statistics:")
        print(f"\tBrockers:\t{kafka_brokers}")
        print(f"\tTopic:\t{kafka_topic_name}")
        print(f"\tPartitions:\t{kafka_partitions}")
        print(f"\tConsumer group id:\t{kafka_consumer_group_id}")

        print(f"\nStart reading from topic \"{kafka_topic_name}\"...")
        counter: int = 0

        msg: KafkaMessage
        for msg in consumer:
            counter += 1
            topic_positions: str = ""
            for p in kafka_topic_partitions:
                topic_positions += f"[part.# {p.partition} - {consumer.position(p)} " \
                                   f"(min. = {kafka_min_offsets[p]} / max. = {kafka_max_offsets[p]})] "

            print(f"\n - packet #{counter} - ")
            print(f"\tkey - {msg.key.decode('utf-8')}")
            print(f"\tvalue - {msg.value}")
            print(f"\tpartition - {msg.partition}")
            print(f"\toffset - {topic_positions}")
            print(f"\n - end -")

            app_logger.get.info(f"Msg. #{counter} read - key = {msg.key.decode('utf-8')} / "
                                     f"value = {msg.value.decode('utf-8')} / "
                                     f"partition = {msg.partition} / "
                                     f"offset = {topic_positions}")

            command = input("Command: Next (press \"Enter\"), Commit (C), Seek (S), Quit (Q)\t")

            if command == "Q":
                app_logger.get.info("user enter Quit(Q) command")
                consumer.close()
                exit(0)

            elif command == "C":
                app_logger.get.info("user enter Commit(C) command")
                consumer.commit()

            elif command == "S":
                app_logger.get.info("user enter Seek(S) command")

                while True:
                    try:
                        print("\n<< Enter additional parameters for seek operation >>")
                        partition_numbers: List[int] = [tp.partition for tp in kafka_topic_partitions]
                        partition_to_seek = int(
                            input(f"\tChoose partition number({', '.join([str(p) for p in partition_numbers])}):\t"))
                        assert partition_to_seek in partition_numbers, f"Partition must be in topic {kafka_topic_name}."

                        tp = TopicPartition(kafka_topic_name, partition_to_seek)
                        offset_bounds: Tuple[int] = (kafka_min_offsets[tp], kafka_max_offsets[tp])
                        position_to_seek = int(
                            input(f"\tEnter position in topic ({offset_bounds[0]}<...<{offset_bounds[1]}):\t"))
                        assert offset_bounds[0] < position_to_seek < offset_bounds[
                            1], f"Offset must be in range {offset_bounds}"
                        print("<< end >>")

                        app_logger.get.info(f"seeking to (topic = {tp}, pos. = {position_to_seek})")
                        consumer.seek(tp, position_to_seek)
                        break
                    except AssertionError as e:
                        print("\n**ERROR** You have passed invalid parameters. Try again...\n")
                        continue

        app_logger.get.info("Topic is empty or connection is lost. Reconnecting...")
        print("Topic is empty or connection is lost. Reconnecting...")
