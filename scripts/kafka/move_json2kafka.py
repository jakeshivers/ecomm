# this script needs confluent-kafka library to run
# pip install confluent-kafka

from confluent_kafka import Producer
import json

# Kafka configuration (replace placeholders with your actual values)
kafka_config = {
    'bootstrap.servers': 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',  # Your Confluent Cloud bootstrap server
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'RDZAT3NLRLPOVPPA',  # Replace with your API key
    'sasl.password': 'W/kJA+Jz8KIX/PHaRU8qEt66DsOpXykI27aHBnyS6jWO3Y1n2POkQ4LDtAp7r2og',  # Replace with your API secret
    'client.id': 'json-uploader'
    #, 'debug': 'security,broker'
}

# Initialize the Kafka producer
producer = Producer(kafka_config)

# Kafka topic where the data will be sent
topic = 'shop-orders-topic'  # Replace with your desired Kafka topic name

# JSON file to read
jsonl_file = '/Users/ruimgcorreia/Documents/Studies/Code/Git/ecomm/data/shopify_orders.jsonl'  # Replace with the actual file path

def delivery_report(err, msg):
    """Callback for producer delivery reports."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Reading and processing the JSONL file
try:
    with open(jsonl_file, "r") as f:  # Use your actual file name
        for line in f:
            # Each line is an independent JSON object
            json_data = json.loads(line)

            # Convert to string and send to Kafka
            producer.produce(topic, json.dumps(json_data).encode("utf-8"), callback=delivery_report)

    producer.flush()  # Ensure all messages are delivered
    print("JSONL data successfully uploaded to Kafka.")

except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
except Exception as e:
    print(f"An error occurred: {e}")