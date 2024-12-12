from flask import Flask, jsonify, request
import os
from dotenv import load_dotenv
import uuid
import random
import stripe

# Load environment variables
load_dotenv()

# Set Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")

app = Flask(__name__)

# Webhook secret from Stripe Dashboard
WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")


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
        return session

    except Exception as e:
        print(f"Error: {e}")
        return None


@app.route("/webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except ValueError as e:
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError as e:
        return "Invalid signature", 400

    # Handle checkout.session.completed event
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        print(f"Webhook received for session: {session['id']}")
        print(f"Metadata: {session.get('metadata')}")

    return jsonify(success=True)


if __name__ == "__main__":
    # Start Flask server for webhook testing
    app.run(port=5000)
