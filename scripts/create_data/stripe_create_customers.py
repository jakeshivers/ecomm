import os
import logging
import json
from dotenv import load_dotenv
from faker import Faker
import stripe
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_API_KEY,
    sasl_plain_password=KAFKA_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Initialize Faker to generate random data
fake = Faker()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Function to create a Stripe customer
def create_customer():
    try:
        customer = stripe.Customer.create(
            email=fake.email(),
            name=fake.name(),
            address={
                "line1": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "postal_code": fake.zipcode(),
                "country": fake.country_code(),
            },
        )
        logger.info(f"Created customer: {customer['id']}")
        return customer
    except Exception as e:
        logger.error(f"Error creating customer: {e}")
        raise


# Function to send events to Kafka
def send_to_kafka(topic, event):
    try:
        producer.send(topic, value=event)
        producer.flush()
        logger.info(f"Sent event to Kafka topic {topic}: {json.dumps(event, indent=2)}")
    except Exception as e:
        logger.error(f"Error sending event to Kafka: {e}")
        raise


# Function to attach the pre-existing test payment method to the customer
def attach_payment_method_to_customer(customer_id):
    try:
        # Using a pre-existing test payment method
        payment_method = stripe.PaymentMethod.retrieve("pm_card_amex")

        # Attach the payment method to the customer
        stripe.PaymentMethod.attach(payment_method.id, customer=customer_id)

        # Set the payment method as the default for the customer
        stripe.Customer.modify(
            customer_id, invoice_settings={"default_payment_method": payment_method.id}
        )
        logger.info(
            f"Attached payment method: {payment_method['id']} to customer {customer_id}"
        )
        return payment_method
    except Exception as e:
        logger.error(f"Error attaching payment method to customer {customer_id}: {e}")
        raise


# Function to create a payment intent (charge) for the customer
def create_payment_intent(customer_id, payment_method_id):
    try:
        # Create a PaymentIntent (charge) for the customer using the attached payment method
        intent = stripe.PaymentIntent.create(
            amount=fake.random_int(
                min=9000, max=500000
            ),  # Amount in cents (e.g., $5 to $200)
            currency="usd",
            customer=customer_id,
            description=f"Purchase from {fake.name()}",
            payment_method=payment_method_id,  # Use the correct payment method ID
            confirm=True,  # Confirm immediately after creating the payment intent
            automatic_payment_methods={
                "enabled": True,
                "allow_redirects": "never",  # This prevents redirect-based payment methods
            },
        )
        logger.info(
            f"Created payment intent: {intent['id']} for ${intent['amount'] / 100:.2f}"
        )
        return intent
    except Exception as e:
        logger.error(f"Error creating payment intent for customer {customer_id}: {e}")
        raise


# Function to create a Checkout Session
def create_checkout_session(customer_id):
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="payment",
            customer=customer_id,
            line_items=[
                {
                    "price_data": {
                        "currency": "usd",
                        "product_data": {
                            "name": fake.catch_phrase(),
                        },
                        "unit_amount": fake.random_int(
                            min=1000, max=50000
                        ),  # Amount in cents
                    },
                    "quantity": fake.random_int(min=1, max=5),
                }
            ],
            success_url="https://example.com/success",  # Replace with your success URL
            cancel_url="https://example.com/cancel",  # Replace with your cancel URL
        )
        logger.info(
            f"Created checkout session: {session['id']} for customer {customer_id}"
        )
        return session
    except Exception as e:
        logger.error(f"Error creating checkout session for customer {customer_id}: {e}")
        raise


# Main function to generate customers, simulate PaymentIntents, and Checkout Sessions
def main(num_customers=10):
    try:
        for i in range(num_customers):
            logger.info(f"Processing customer {i + 1} of {num_customers}")
            try:
                # Step 1: Create a customer
                customer = create_customer()

                # Step 2: Attach the pre-existing test payment method to the customer
                payment_method = attach_payment_method_to_customer(customer["id"])

                # Step 3: Create a PaymentIntent (charge) for the customer
                intent = create_payment_intent(customer["id"], payment_method["id"])

                # Step 4: Create a Checkout Session for the customer
                session = create_checkout_session(customer["id"])

                # Step 5: Enrich the event and send to Kafka
                customer_event = {
                    "customer_id": customer["id"],
                    "email": customer["email"],
                    "name": customer["name"],
                    "address": customer["address"],
                    "payment_intent_id": intent["id"],
                    "checkout_session_id": session["id"],
                }
                send_to_kafka(KAFKA_TOPIC, customer_event)
            except Exception as e:
                logger.error(f"Error processing customer {i + 1}: {e}")
    finally:
        # Close the producer gracefully with a timeout
        logger.info("Closing the Kafka producer with 10 seconds timeout.")
        producer.close(timeout=10)


if __name__ == "__main__":
    main(num_customers=10)  # Change num_customers to generate more
