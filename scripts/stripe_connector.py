import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()


def fetch_stripe_transactions():
    # Stripe API URL
    url = "https://api.stripe.com/v1/charges"

    # Authorization header with API key
    headers = {"Authorization": f'Bearer {os.getenv("STRIPE_API_KEY")}'}

    # Calculate Unix timestamp for 30 days ago
    thirty_days_ago = datetime.now() - timedelta(days=30)
    timestamp_30_days_ago = int(thirty_days_ago.timestamp())

    # Request parameters
    params = {"limit": 100, "created[gte]": timestamp_30_days_ago}

    print("Headers:", headers)
    print("URL:", url)

    # Make the request to Stripe API
    response = requests.get(url, headers=headers, params=params)

    # Check response status and handle it
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch Stripe transactions: {response.status_code}")


# Call the function
print("hello")
transactions = fetch_stripe_transactions()
print(transactions)
