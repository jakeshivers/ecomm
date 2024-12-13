"""
    ORDERS FETCHING AND LOGGING SCRIPT
    If you want to see new orders, you need to manually change them from

"""

import os
import json
import logging
import shopify
from dotenv import load_dotenv
import boto3

# Load environment variables
load_dotenv()

# Import exceptions from botocore
from botocore.exceptions import NoCredentialsError, ClientError

# Shopify API credentials
SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")

# Create log directory if it doesn't exist
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
    print(f"Directory created at: {log_dir}")  # Debugging statement
else:
    print(f"Directory already exists at: {log_dir}")  # Debugging statement


# Define log file path
log_file_path = os.path.join(log_dir, "fetch_and_log_orders.log")
print(f"Log file path: {log_file_path}")  # Debugging statement

# Configure logging
try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info("Logging initialized successfully.")
except Exception as e:
    print(f"Error initializing logging: {e}")
    raise


# Initialize Shopify session
def initialize_shopify_session():
    shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
    session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
    shopify.ShopifyResource.activate_session(session)
    logger.info("Shopify session initialized successfully.")


# Write order data to JSONL file
def write_to_jsonl_file(order_data, output_file="../../data/shopify_orders.jsonl"):
    try:
        with open(output_file, "a") as file:
            file.write(json.dumps(order_data) + "\n")
        logger.info(
            f"Order {order_data.get('id', 'unknown')} written to {output_file}."
        )
    except Exception as e:
        logger.error(f"Failed to write order data: {e}")


# Fetch all orders from Shopify
def fetch_all_orders():
    logger.info("Fetching all orders from Shopify...")
    orders_fetched = 0
    last_order_id = None  # Cursor for pagination

    while True:
        try:
            # Define query parameters
            query_params = {"limit": 50}
            if last_order_id:
                query_params["since_id"] = last_order_id

            # Fetch orders
            orders = shopify.Order.find(**query_params)
            if not orders:
                logger.info("No more orders to fetch.")
                break

            # Process and save orders
            for order_obj in orders:
                try:
                    # Handle nested structure
                    order_data = (
                        json.loads(order_obj.to_json())
                        if hasattr(order_obj, "to_json")
                        else order_obj
                    )

                    # Check for nested 'order' key and handle accordingly
                    if "order" in order_data:
                        order_data = order_data["order"]

                    # Validate the presence of the 'id' field
                    if "id" not in order_data:
                        logger.warning(f"Order missing 'id': {order_data}")
                        continue

                    write_to_jsonl_file(order_data)
                    last_order_id = order_data["id"]  # Update the cursor
                    orders_fetched += 1

                except Exception as inner_e:
                    logger.error(f"Error processing an order: {inner_e}")

            logger.info(
                f"Fetched and logged {len(orders)} orders. Total so far: {orders_fetched}."
            )

            # Break if fewer than the maximum batch size is returned
            if len(orders) < 50:
                logger.info("Reached the last page of orders.")
                break

        except Exception as e:
            logger.error(f"Error fetching orders: {e}")
            break

    logger.info(f"Total orders fetched and logged: {orders_fetched}")


# Function to upload file to S3
def upload_to_s3(local_file_path, bucket_name, s3_key):

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"File successfully uploaded to S3: s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("Error: No AWS credentials found.")
    except ClientError as e:
        print(f"Error uploading to S3: {e}")


# Main function
def main():
    initialize_shopify_session()
    fetch_all_orders()
    # Upload the local file to S3
    local_file_path = "../../data/shopify_orders.jsonl"
    bucket_name = "capstone-shopify"
    s3_key = "shopify_orders.jsonl"

    upload_to_s3(local_file_path, bucket_name, s3_key)


if __name__ == "__main__":
    main()
