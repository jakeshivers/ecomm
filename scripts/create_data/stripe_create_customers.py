import stripe
import os
from faker import Faker
from dotenv import load_dotenv

# Load environment variables (you should have your Stripe API key here)
load_dotenv()

# Set Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")

# Initialize Faker to generate random data
fake = Faker()


# Function to create a Stripe customer
def create_customer():
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
    return customer


# Function to attach the pre-existing test payment method (e.g., pm_card_visa) to the customer
def attach_payment_method_to_customer(customer_id):
    # Using a pre-existing test payment method
    payment_method = stripe.PaymentMethod.retrieve("pm_card_amex")

    # Attach the payment method to the customer
    stripe.PaymentMethod.attach(payment_method.id, customer=customer_id)

    # Set the payment method as the default for the customer
    stripe.Customer.modify(
        customer_id, invoice_settings={"default_payment_method": payment_method.id}
    )
    return payment_method


# Function to create a payment intent (charge) for the customer
def create_payment_intent(customer_id, payment_method_id):
    # Create a PaymentIntent (charge) for the customer using the attached payment method
    intent = stripe.PaymentIntent.create(
        amount=fake.random_int(
            min=900000, max=50000000
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
    return intent


# Main function to generate customers and charges
def main(num_customers=10):
    for _ in range(num_customers):
        # Step 1: Create a customer
        customer = create_customer()
        print(f"Created customer: {customer['id']}")

        # Step 2: Attach the pre-existing test payment method to the customer
        payment_method = attach_payment_method_to_customer(customer["id"])
        print(
            f"Attached payment method: {payment_method['id']} to customer {customer['id']}"
        )

        # Step 3: Create a PaymentIntent (charge) for the customer
        intent = create_payment_intent(customer["id"], payment_method["id"])
        print(
            f"Created payment intent: {intent['id']} for ${intent['amount'] / 100:.2f}"
        )


if __name__ == "__main__":
    main(num_customers=3)  # Change num_customers to generate more
