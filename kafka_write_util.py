# task specified imports
import json

from kafka import KafkaProducer

# custom data structures
from film_record import FilmRecord
from logger import CustomLogger
# additional modules for better UX
from utils import Gauge

# Global presets
file_name = "C:\\Users\\avikulin\\Desktop\\movies.json"
kafka_brockers = ["10.40.1.142:9092", "10.40.1.141:9092"]
kafka_topic_name = "avikulin_test"
kafka_clientid = "Python test util"
kafka_value_serializer = FilmRecord.serialize

# ! Executed code
if __name__ == "__main__":
    logger_instance = CustomLogger("kafka_wtire_util")
    logger_instance.activate()

    films_store = list()

    logger_instance.get.info(f"Start reading data from file {file_name}.")
    with open(file_name, mode="r") as source_file:
        data_store = json.load(source_file)
        print("JSON loaded.")
        for i, item in enumerate(data_store):
            films_store.append(FilmRecord.decode(item))

        print(f"Statistics: count ={len(data_store)}, collection type = {type(data_store)}")
        print(f"Film store: count = {len(films_store)}, item type = {type(films_store[0]) if len(
            films_store) > 0 else 'None'}")

        logger_instance.get.info("Data successfully loaded into memory.")

    logger_instance.get.info(f"Init Kafka producer for connection to {kafka_brockers}.")
    producer = KafkaProducer(bootstrap_servers=kafka_brockers,
                             client_id=kafka_clientid,
                             value_serializer=kafka_value_serializer,
                             acks=1)
    logger_instance.get.info("Kafka producer successfully inited.")

    gauge = Gauge("Sending data to Apache Kafka", len(films_store))

    logger_instance.get.info(f"Begin sending data to Kafka topic {kafka_topic_name}")
    for i, item in enumerate(films_store):
        logger_instance.get.info(f"Data: #{str(i)} - {str(item)}")
        producer.send(kafka_topic_name, key=bytes(str(i), "utf-8"), value=item)
        logger_instance.get.info(f"Message #{i} has been sent...\n")
        gauge.show_progress(i + 1)

    producer.flush()
    producer.close()
