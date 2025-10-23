import time 
import json 
import random 
from api_calling import openweathermap
from datetime import datetime
from data_generator import generate_message
from kafka import KafkaProducer

TOPIC_NAME = 'messages'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda key: key.encode('utf-8'),
    value_serializer=lambda message: message.encode('utf-8')
)

if __name__ == '__main__':
    key_id = 0
    # Infinite loop - runs until you kill the program
    while True:
        # Generate a message
        dummy_message = json.dumps(generate_message())
        
        # Call openweather api
        openweather_data = json.dumps(openweathermap('Ha Noi'))
        
        # Send it to our 'messages' topic
        print(f'Producing message @ {datetime.now()} | Key = {key_id} | Message = {str(openweather_data)}')
        producer.send(topic = TOPIC_NAME, key=str(key_id), value = openweather_data)
        
        # Sleep for a number of seconds
        time.sleep(10)
        key_id += 1
        
        # Send it to our 'messages' topic
        print(f'Producing message @ {datetime.now()} | Key = {key_id} | Message = {str(dummy_message)}')
        producer.send(topic = TOPIC_NAME, key=str(key_id), value = dummy_message)
        
        # Sleep for a number of seconds
        time.sleep(10)
        key_id += 1
