import stripe
from flask import Flask, jsonify, request
import os
from dotenv import load_dotenv
import subprocess

# Load environment variables (you should have your Stripe API key here)
load_dotenv()

# Set Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")


def generate_random_metadata():
    return {
        "user_id": str(uuid.uuid4()),
        "order_id": f"ORD-{random.randint(1000, 9999)}",
        "custom_field": f"custom_value_{random.choice(['A', 'B', 'C', 'D'])}",
    }


def trigger_checkout_session():
    try:
        # Generate random metadata
        metadata = generate_random_metadata()

        # Randomize product name and price
        product_name = random.choice(
            ["Test Product A", "Test Product B", "Test Product C"]
        )
        unit_amount = random.choice([1000, 2000, 3000, 5000])

        # Create a checkout session
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[
                {
                    "price_data": {
                        "currency": "usd",
                        "product_data": {
                            "name": product_name,
                        },
                        "unit_amount": unit_amount,
                    },
                    "quantity": random.randint(1, 5),
                },
            ],
            mode="payment",
            success_url="https://example.com/success",
            cancel_url="https://example.com/cancel",
            metadata=metadata,
        )

        print(f"Created session ID: {session.id}")

        # Simulate the checkout.session.completed event using Stripe CLI
        result = subprocess.run(
            [
                "stripe",
                "trigger",
                "checkout.session.completed",
                "--add",
                f"id={session.id}",
            ],
            capture_output=True,
            text=True,
        )

        print(f"CLI Output: {result.stdout}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    trigger_checkout_session()
