from kafka import KafkaConsumer
from config import BROKER_EXTERNAL_PORT, TOPIC

consumer = KafkaConsumer(
    TOPIC, 
    bootstrap_servers=f'localhost:{BROKER_EXTERNAL_PORT}'
)

for message in consumer:
    print (message.value.decode('utf-8'))
