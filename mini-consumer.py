from kafka import KafkaConsumer
from sys import exit
print("Apache Kafka minimal consumer utility\n")

server = input("Server (ip:port):\t")
group = input("Group name:\t")
topic = input("Topic name:\t")
policy = input("Read policy (1 - latest, 0-earliest):\t")

while True:
    print('\nConnecting to kafka...')
    consumer = KafkaConsumer(
        topic,
        group_id=group,
        bootstrap_servers=[server],
        request_timeout_ms=6001,
        session_timeout_ms=6000,
        heartbeat_interval_ms=2000,
        auto_offset_reset="latest" if policy == 1 else "earliest",
        enable_auto_commit=False
    )

    count = 0
    print("Waiting messages...")
    for message in consumer:
        print(f"#{count} - {message.value.decode('utf-8')}\t")
        count = count + 1
        key = input("Next - Enter, Quite - Q:\t")
        if key == "Q":
            exit(0)
    print('Topic is empty or connection is lost. Reconnecting...')