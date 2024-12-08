import requests
import os


def fetch_stripe_transactions():
    url = "https://api.stripe.com/v1/charges"
    headers = {"Authorization": f'Bearer {os.getenv("STRIPE_API_KEY")}'}
    params = {"limit": 100, "created[gte]": "30 days ago"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch Stripe transactions: {response.status_code}")
