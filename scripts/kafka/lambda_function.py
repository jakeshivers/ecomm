import json
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
KAFKA_TOPIC = "stripe"
KAFKA_API_KEY = "6TRBTMGQ2H3HT6QS"
KAFKA_API_SECRET = "i2qfC0RveTJbqk8RIvoNvVSWGXonGszgEqO9LaM3zotYMXROgHxrsAfyF+GRa6Dd"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_API_KEY,
    sasl_plain_password=KAFKA_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def lambda_handler(event, context):
    try:
        # Log the received EventBridge event
        print("Received Event:", json.dumps(event, indent=2))

        # Extract the EventBridge detail field
        stripe_event = event.get("detail", {})

        # Produce the event to Kafka
        producer.send(KAFKA_TOPIC, value=stripe_event)
        producer.flush()  # Ensure the event is sent

        print(f"Event sent to Kafka topic {KAFKA_TOPIC}: {stripe_event}")

        return {
            "statusCode": 200,
            "body": json.dumps("Event successfully sent to Kafka."),
        }

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Error: {e}")}
