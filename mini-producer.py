from kafka import KafkaProducer

print("Apache Kafka minimal producer utility\n")

server = input("Server (ip:port):\t")
topic = input("Topic name:\t")
producer = KafkaProducer(bootstrap_servers=[server])

print("Start sending...")
while True:
    message = input("Message body (Q - quite):\t")
    if message == "Q":
        break
    producer.send(topic, message.encode('utf-8'))

producer.close()
