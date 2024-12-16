"""
    ORDERS FETCHING AND LOGGING SCRIPT
    Fetch Shopify orders and save them to a file with the passed date in the file name.
"""

import os
import json
import logging
import shopify
from dotenv import load_dotenv
import boto3
import sys
from datetime import datetime, timedelta
import pytz

# Load environment variables
load_dotenv()

# Import exceptions from botocore
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime, timedelta

# Shopify API credentials
SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")

# Create log directory if it doesn't exist
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(log_dir, exist_ok=True)

# Define log file path
log_file_path = os.path.join(log_dir, "fetch_and_log_orders.log")

# Configure logging
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


# Initialize Shopify session
def initialize_shopify_session():
    shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
    session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
    shopify.ShopifyResource.activate_session(session)
    logger.info("Shopify session initialized successfully.")


# Write order data to JSONL file
def write_to_jsonl_file(order_data, output_file):
    try:
        # Check if the file exists and delete it if this is the first write
        if not hasattr(write_to_jsonl_file, "file_checked"):
            if os.path.exists(output_file):
                os.remove(output_file)
                logger.info(f"Existing file {output_file} deleted.")
            # Mark as checked to avoid deleting on subsequent writes
            write_to_jsonl_file.file_checked = True

        with open(output_file, "a") as file:
            file.write(json.dumps(order_data) + "\n")
        logger.info(
            f"Order {order_data.get('id', 'unknown')} written to {output_file}."
        )
    except Exception as e:
        logger.error(f"Failed to write order data: {e}")


# Fetch all orders from Shopify
def fetch_all_orders(date_str, output_file):

    orders_fetched = 0
    last_order_id = None  # Cursor for pagination

    logger.info(f"Fetching all orders for date: {date_str}...")

    # Parse and set the date range in the store's local timezone
    store_timezone = pytz.timezone(
        "America/Denver"
    )  # Replace with your store's timezone
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    start_datetime_local = store_timezone.localize(
        datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0)
    )
    end_datetime_local = start_datetime_local + timedelta(days=1)

    while True:
        try:
            # Define query parameters
            query_params = {"limit": 50}
            if last_order_id:
                query_params["since_id"] = last_order_id

            # Parse the input date string and set the date range
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            created_at_min = start_datetime_local.astimezone(pytz.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            created_at_max = end_datetime_local.astimezone(pytz.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            print(
                f"Using UTC date range: created_at_min={created_at_min}, created_at_max={created_at_max}"
            )

            # Fetch orders
            orders = shopify.Order.find(
                limit=250,
                created_at_min=created_at_min,
                created_at_max=created_at_max,
            )
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

                    write_to_jsonl_file(order_data, output_file)
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
    if len(sys.argv) != 2:
        logger.error("Please provide a date in yyyy-mm-dd format.")
        sys.exit(1)

    date_str = sys.argv[1]

    # Validate the input date format
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(
            "Incorrect date format. Please provide a date in yyyy-mm-dd format."
        )
        sys.exit(1)

    # Ensure the output directory exists
    data_dir = os.path.abspath("../../data")
    os.makedirs(data_dir, exist_ok=True)

    output_file = os.path.join(data_dir, f"shopify_orders_{date_str}.jsonl")

    initialize_shopify_session()
    fetch_all_orders(date_str, output_file)  # Pass both arguments

    if os.path.exists(output_file):
        # Upload the local file to S3
        bucket_name = "capstone-shopify"
        s3_key = f"shopify_orders_{date_str}.jsonl"
        upload_to_s3(output_file, bucket_name, s3_key)


if __name__ == "__main__":
    main()
