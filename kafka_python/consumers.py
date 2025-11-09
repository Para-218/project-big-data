import json
import csv
import os
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

    # Create file csv
    with open('data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 'sea_level', 'grnd_level', 'visibility', 'speed', 'deg', 'gust', 'cloud', 'datetime'])
        csvfile.close()

    # Write into csv for every messages
    for message in consumer:
        my_bytes_value = message.value
        my_json = json.loads(my_bytes_value.decode('utf8').replace("'", '"'))
        with open('data.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([my_json['main']['temp'], my_json['main']['feels_like'], my_json['main']['temp_min'], my_json['main']['temp_max'], my_json['main']['pressure'], my_json['main']['humidity'], my_json['main']['sea_level'], my_json['main']['grnd_level'], my_json['visibility'], my_json['wind']['speed'], my_json['wind']['deg'], my_json['wind']['gust'], my_json['clouds']['all'], my_json['dt']])
            csvfile.close()
        