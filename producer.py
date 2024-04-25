import json
import time
import requests
from kafka import KafkaProducer
from config import BROKER_EXTERNAL_PORT, TOPIC

# from confluent_kafka import Producer

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=f'localhost:{BROKER_EXTERNAL_PORT}'
)

# API endpoint URL
api_url = "https://api.coindesk.com/v1/bpi/currentprice.json"

# Function to fetch data from API
def fetch_data():
    response = requests.get(api_url)
    
    if response.status_code == 200:
        print("==> API response: ", response.json())
        return response.json()
    else:
        print(f"Failed to fetch data from API. Status code: {response.status_code}")
        return None

# Function to send data to Kafka
def send_to_kafka(topic, data):
    try:
        producer.send(topic, json.dumps(data).encode('utf-8'))
        producer.flush()
    except KeyboardInterrupt:
        producer.close()

def employees_data():
    return {
            "id": "1",
            "first_name": "John",
            "last_name": "Doe"
    }

# Main function
def main():
    cnt = 0
    while cnt < 2:
        try:
            # Fetch data from API
            # data = fetch_data()
            data = employees_data() # hard coded
            print(f"==> data: {json.dumps(data)}")
            if data:
                # Send data to Kafka
                send_to_kafka(TOPIC, data)
                print("Data sent to Kafka successfully.")
        except KeyboardInterrupt:
            producer.close()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            cnt += 1
            # Sleep for some time before fetching data again
            time.sleep(60)  # Adjust as needed

if __name__ == "__main__":
    main()
