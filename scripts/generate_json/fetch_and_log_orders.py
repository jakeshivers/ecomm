import os
import json
import logging
import shopify
from dotenv import load_dotenv
import boto3
from datetime import datetime, timedelta
import pytz
import sys

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
def write_to_jsonl_file(order_data, output_file):
    try:
        with open(output_file, "w") as file:
            file.write(json.dumps(order_data) + "\n")
        logger.info(
            f"Order {order_data.get('id', 'unknown')} written to {output_file}."
        )
    except Exception as e:
        logger.error(f"Failed to write order data: {e}")


# Fetch orders from Shopify by date, including pagination handling
def fetch_orders_by_date(date_str):
    logger.info(f"Fetching orders for {date_str}...")
    orders_fetched = 0
    orders = []

    # Parse the input date string and set the start and end datetime in UTC
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    utc_timezone = pytz.utc

    # Define the start and end of the day in UTC
    start_date_utc = utc_timezone.localize(
        datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0)
    )  # Start of the day in UTC
    end_date_utc = start_date_utc + timedelta(days=1)  # Start of the next day in UTC

    # Convert to ISO 8601 format
    start_date = start_date_utc.isoformat()
    end_date = end_date_utc.isoformat()

    # Log the date range being used
    logger.info(f"Using date range: {start_date} to {end_date}")

    # Generate output file path based on the provided date
    output_file = f"../../data/shopify_orders_{date_str}.jsonl"

    try:
        # Define query parameters
        query_params = {
            "limit": 50,
            "created_at_min": start_date,
            "created_at_max": end_date,
        }

        # Fetch the first page of orders
        orders_page = shopify.Order.find(**query_params)
        logger.info(f"Fetched {len(orders_page)} orders on the first page.")

        while orders_page:
            for order in orders_page:
                order_data = (
                    json.loads(order.to_json()) if hasattr(order, "to_json") else order
                )
                orders.append(order_data)  # Collect all orders for debugging
                write_to_jsonl_file(order_data, output_file)
                orders_fetched += 1
                logger.info(f"Order fetched: ID {order_data.get('id')}")

            # Check if there is a next page
            if not orders_page.has_next_page():
                break

            orders_page = orders_page.next_page()
            logger.info(f"Fetched next page of orders.")

        logger.info(f"Total orders fetched for {date_str}: {orders_fetched}")
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")

    # Log all orders fetched for debugging
    logger.debug(f"All orders fetched for {date_str}: {json.dumps(orders, indent=2)}")


# Function to upload file to S3
def upload_to_s3(local_file_path, bucket_name, s3_key):
    # Check if the local file exists
    if not os.path.exists(local_file_path):
        logger.error(f"Local file {local_file_path} does not exist.")
        return

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"File successfully uploaded to S3: s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        logger.error("Error: No AWS credentials found.")
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")


if order_data:
    print(f"Order Data: {json.dumps(order_data, indent=2)}")
else:
    print(f"Order {order_id} could not be retrieved.")


# Main function
def main():
    if len(sys.argv) != 2:
        logger.error("Please provide a date in yyyy-mm-dd format.")
        sys.exit(1)

    date_str = sys.argv[1]
    print(date_str)

    # Validate the input date format
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(
            "Incorrect date format. Please provide a date in yyyy-mm-dd format."
        )
        sys.exit(1)

    # Initialize session and fetch orders for the given date
    initialize_shopify_session()
    fetch_orders_by_date(date_str)

    # Upload the local file to S3
    local_file_path = f"../../data/shopify_orders_{date_str}.jsonl"
    bucket_name = "capstone-shopify"
    s3_key = f"shopify_orders_{date_str}.jsonl"  # Use the same date in the S3 file key
    upload_to_s3(local_file_path, bucket_name, s3_key)


if __name__ == "__main__":
    main()
