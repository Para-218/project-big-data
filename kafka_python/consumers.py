import json 
from kafka import KafkaConsumer

TOPIC_NAME = 'messages'
BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest'
    )
    for message in consumer:
        my_bytes_value = message.value
        my_json = my_bytes_value.decode('utf8').replace("'", '"')
        print(json.loads(my_json))
