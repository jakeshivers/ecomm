import requests
import os


def fetch_shopify_orders():
    url = "https://yourshopname.myshopify.com/admin/api/2023-01/orders.json"
    params = {
        "status": "any",  # Specify order status if necessary
        "created_at_min": "30 days ago",
    }
    auth = (os.getenv("SHOPIFY_API_KEY"), os.getenv("SHOPIFY_PASSWORD"))
    response = requests.get(url, params=params, auth=auth)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch Shopify orders: {response.status_code}")
