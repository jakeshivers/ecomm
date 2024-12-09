import shopify
import pandas as pd
import schedule
import time
import os
from dotenv import load_dotenv
from pyactiveresource.connection import Error

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


def fetch_shopify_data(endpoint):
    """Fetch data from Shopify API"""
    try:
        response = shopify.ShopifyResource.find(endpoint)
        return response
    except Exception as e:
        print(f"Error fetching data from Shopify: {e}")
        return None


def fetch_product_metadata():
    """Fetch metadata for all products in Shopify"""
    try:
        all_products = []
        page = 1

        # Fetch products in a paginated manner
        while True:
            products = shopify.Product.find(
                limit=250, page=page
            )  # Fetch up to 250 products per page
            if not products:
                break  # Stop when no more products are returned
            all_products.extend(products)
            page += 1

        return all_products

    except Error as e:
        print(f"Shopify API error: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def fetch_inventory_levels():
    """Fetch inventory levels from Shopify"""
    inventory = shopify.InventoryLevel.find()
    return inventory if inventory else []


def merge_product_inventory(products, inventory):
    """Merge product metadata and inventory data"""
    # Convert product list to DataFrame
    product_df = pd.json_normalize([product.to_dict() for product in products])

    # Convert inventory data to DataFrame
    inventory_df = pd.json_normalize([inv.to_dict() for inv in inventory])

    # Merge the data based on product ID
    merged_data = pd.merge(
        product_df, inventory_df, left_on="id", right_on="product_id", how="left"
    )

    # Clean the DataFrame (optional, depending on required structure)
    merged_data = merged_data[["id", "title", "variants", "inventory_quantity", "sku"]]

    return merged_data


def store_data(merged_data):
    """Store the merged data (can be modified to store in a database or file)"""
    # Example: Saving data to a CSV file
    merged_data.to_csv("shopify_inventory_and_products.csv", index=False)
    print("Data saved successfully.")


def job():
    """Main job to fetch, merge, and store product & inventory data"""
    print("Fetching product metadata...")
    products = fetch_product_metadata()

    print("Fetching inventory levels...")
    inventory = fetch_inventory_levels()

    if products and inventory:
        print("Merging data...")
        merged_data = merge_product_inventory(products, inventory)

        print("Storing merged data...")
        store_data(merged_data)
    else:
        print("Failed to fetch data.")


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

fetch_product_metadata()
exit()

# Schedule the job to run hourly
schedule.every().minute.do(job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute for scheduled tasks
