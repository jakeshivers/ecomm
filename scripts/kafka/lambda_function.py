import json
import logging
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "your_broker_url")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stripe-customers")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY", "your_api_key")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET", "your_api_secret")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
        # Log the incoming event
        logger.info(f"Received event: {json.dumps(event)}")

        # Validate the event contains required fields
        required_fields = ["id", "email", "created_at"]
        missing_fields = [field for field in required_fields if field not in event]
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {"error": f"Missing required fields: {missing_fields}"}
                ),
            }

        # Extract relevant fields
        customer_id = event["id"]
        email = event["email"]
        created_at = event["created_at"]
        address = event.get("address", {})
        metadata = event.get("metadata", {})
        name = event.get("name", "Unknown")

        # Prepare the message payload
        payload = {
            "id": customer_id,
            "email": email,
            "created_at": created_at,
            "name": name,
            "address": address,
            "metadata": metadata,
        }

        # Send the event to Kafka
        producer.send(KAFKA_TOPIC, value=payload)
        producer.flush()
        logger.info(f"Event sent to Kafka topic '{KAFKA_TOPIC}': {json.dumps(payload)}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Event processed successfully"}),
        }

    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


if __name__ == "__main__":
    with open("test_event.json") as f:
        test_event = json.load(f)
    print(lambda_handler(test_event, None))
