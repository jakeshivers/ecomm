import json
from kafka import KafkaProducer
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "your_broker_url")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stripe-customers")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY", "your_api_key")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET", "your_api_secret")

print(f"KAFKA_BROKER: {KAFKA_BROKER}")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_API_KEY,
    sasl_plain_password=KAFKA_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_API_KEY,
        sasl_plain_password=KAFKA_API_SECRET,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info("Kafka producer initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    raise

# Lambda handler
def lambda_handler(event, context):
    try:
        # Log the received EventBridge event
        logger.info(f"Received Event: {json.dumps(event, indent=2)}")

        # Extract the detail field from the EventBridge event
        event_detail = event.get("detail", {})
        if not event_detail:
            logger.error("No detail field found in the event.")
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid event: Missing detail field."),
            }

        # Send the event detail to Kafka
        producer.send(KAFKA_TOPIC, value=event_detail)
        producer.flush()
        logger.info(f"Event sent to Kafka topic '{KAFKA_TOPIC}': {json.dumps(event_detail, indent=2)}")

        return {
            "statusCode": 200,
            "body": json.dumps("Event successfully processed and sent to Kafka."),
        }

    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing event: {str(e)}"),
        }

    finally:
        try:
            producer.close(timeout=5)
            logger.info("Kafka producer closed.")
        except Exception as close_err:
            logger.warning(f"Failed to close Kafka producer gracefully: {close_err}")