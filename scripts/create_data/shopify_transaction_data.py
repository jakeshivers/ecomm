import shopify
import pandas as pd
import schedule
import time
import os
from dotenv import load_dotenv
from pyactiveresource.connection import Error
from itertools import islice
import json
import datetime

# Load environment variables
load_dotenv()

# Shopify credentials
SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")  # e.g., "capstone-data"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")  # Admin API token


# Initialize Shopify session
def initialize_shopify_session():
    shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
    session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
    shopify.ShopifyResource.activate_session(session)


def chunked_iterable(iterable, size):
    """Helper function to split an iterable into chunks."""
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


def fetch_products_with_inventory():
    """Fetch products with their corresponding inventory levels."""
    try:
        print("Fetching products and inventory levels...")

        # Step 1: Fetch all products
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())  # UTC
        products = shopify.Product.find(limit=250)
        inventory_item_map = {}  # Map of inventory_item_id to product/variant details

        for product in products:
            for variant in product.variants:
                inventory_item_map[variant.inventory_item_id] = {
                    "timestamp": timestamp,
                    "product_title": product.title,
                    "variant_title": variant.title,
                    "sku": variant.sku,
                }

        # Step 2: Fetch inventory levels
        all_inventory_levels = []
        inventory_item_ids = list(inventory_item_map.keys())
        for chunk in chunked_iterable(
            inventory_item_ids, 250
        ):  # Shopify limit is 250 IDs per request
            inventory_levels = shopify.InventoryLevel.find(
                inventory_item_ids=",".join(map(str, chunk))
            )
            all_inventory_levels.extend(inventory_levels)

        # Step 3: Map inventory levels back to products
        for inventory_level in all_inventory_levels:
            item_id = inventory_level.inventory_item_id
            if item_id in inventory_item_map:
                inventory_item_map[item_id]["location_id"] = inventory_level.location_id
                inventory_item_map[item_id]["available"] = inventory_level.available

        # Step 4: Format and print the result
        results = []

        for item in inventory_item_map.values():
            results.append(item)
            print(
                f"Timestamp:  {timestamp} | "
                f"Product: {item['product_title']} | Variant: {item['variant_title']} | "
                f"SKU: {item['sku']} | Location ID: {item.get('location_id', 'N/A')} | "
                f"Available: {item.get('available', 'N/A')}"
            )
        # Write each record as its own JSON line
        with open("shopify_inventory_and_products.json", "w") as f:
            for record in results:
                f.write(json.dumps(record) + "\n")

        return results  # Return for further processing if needed

    except Error as ve:
        print(f"Validation error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def fetch_orders_last_day():
    """Fetch orders created in the last 24 hours"""
    try:
        # Calculate the timestamp for 24 hours ago
        one_day_ago = datetime.datetime.now() - datetime.timedelta(days=1)
        one_day_ago = (
            one_day_ago.isoformat()
        )  # Convert to ISO format string (required by Shopify)

        # Fetch orders created in the last 24 hours (no pagination loop)
        orders = shopify.Order.find(created_at_min=one_day_ago, limit=250, status="any")

        if not orders:
            print("No orders found in the last 24 hours.")
            return

        results = []

        # Print the details of each order and collect them in results list
        for order in orders:
            # Extract relevant fields from the order
            order_data = {
                "order_id": order.id,
                "created_at": order.created_at,
                "status": order.financial_status,
                "total": order.total_price,
                "customer_name": (
                    f"{order.customer.first_name} {order.customer.last_name}"
                    if order.customer
                    else "N/A"
                ),
            }

            # Print the order details
            print(
                f"Order ID: {order_data['order_id']} | "
                f"Created At: {order_data['created_at']} | "
                f"Status: {order_data['status']} | "
                f"Total: {order_data['total']} | "
                f"Customer: {order_data['customer_name']}"
            )

            # Add order data to results list
            results.append(order_data)

        # Write the order data to a JSON file
        with open("shopify_orders.json", "w") as f:
            for record in results:
                f.write(json.dumps(record) + "\n")

    except Error as e:
        print(f"Shopify API error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in fetch_orders_last_day(): {e}")


# Initialize Shopify session
try:
    # Initialize session
    initialize_shopify_session()
    print("Shopify session initialized successfully.")

    # Test fetching shop info
    shop = shopify.Shop.current()
    print(f"Connected to Shopify store: {shop.name}")

except Exception as e:
    print(f"Error initializing Shopify session: {e}")


# TODO schedule the job to run every X period of time


def job():
    print("Running scheduled job...")

    fetch_products_with_inventory()
    print("Fetched products with inventory.")

    fetch_orders_last_day()
    print("Fetched orders from the last 24 hours.")


# Schedule the job to run hourly
schedule.every().minute.do(job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute for scheduled tasks
