import os
import requests
import logging
import json
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")
SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
SHOPIFY_PASSWORD = os.getenv("SHOPIFY_PASSWORD")
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")

# Constants
REQUEST_DELAY = 1  # Seconds between requests
MAX_RETRIES = 3  # Number of retry attempts for failed requests

# Set up logging
log_folder = "logs"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_file = os.path.join(log_folder, "data_ingestion.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

logger = logging.getLogger()


def create_session():
    """
    Creates a requests session with retry logic to handle transient errors and rate limits.
    """
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_shopify_orders(
    session, shopify_store_name, shopify_api_key, shopify_password
):
    """
    Fetches Shopify orders from the past 30 days.

    Args:
        session (requests.Session): The requests session.
        shopify_store_name (str): Shopify store name without '.myshopify.com'.
        shopify_api_key (str): Shopify API key.
        shopify_password (str): Shopify API password (Admin API access token).

    Returns:
        list: A list of Shopify order dictionaries.
    """
    orders = []
    base_url = (
        f"https://{shopify_store_name}.myshopify.com/admin/api/2023-10/orders.json"
    )
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    params = {
        "status": "any",
        "created_at_min": thirty_days_ago,
        "limit": 250,  # Maximum allowed by Shopify
    }

    while True:
        try:
            response = session.get(
                base_url, params=params, auth=(shopify_api_key, shopify_password)
            )
            logger.info(f"GET {response.url} - Status Code: {response.status_code}")
            if response.status_code != 200:
                logger.error(
                    f"Failed to fetch Shopify orders: {response.status_code} - {response.text}"
                )
                break

            data = response.json()
            fetched_orders = data.get("orders", [])
            orders.extend(fetched_orders)
            logger.info(f"Fetched {len(fetched_orders)} orders.")

            # Check for pagination
            if "link" in response.headers:
                links = requests.utils.parse_header_links(response.headers["link"])
                next_link = None
                for link in links:
                    if link.get("rel") == "next":
                        next_link = link.get("url")
                        break
                if next_link:
                    base_url = next_link
                    params = {}  # Parameters are already included in next_link
                    logger.info("Fetching next page of orders.")
                else:
                    break
            else:
                break
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            logger.error(f"Exception while fetching Shopify orders: {e}")
            break

    logger.info(f"Total Shopify orders fetched: {len(orders)}")
    return orders


def fetch_stripe_payments(session, stripe_api_key):
    """
    Fetches Stripe charges and refunds from the past 30 days.

    Args:
        session (requests.Session): The requests session.
        stripe_api_key (str): Stripe API key.

    Returns:
        tuple: Two lists containing Stripe charges and refunds respectively.
    """
    charges = []
    refunds = []
    base_url_charges = "https://api.stripe.com/v1/charges"
    base_url_refunds = "https://api.stripe.com/v1/refunds"
    thirty_days_ago = int(time.time()) - 30 * 24 * 60 * 60

    # Fetch Charges
    params_charges = {"limit": 100, "created[gte]": thirty_days_ago}

    while True:
        try:
            response = session.get(
                base_url_charges,
                params=params_charges,
                headers={"Authorization": f"Bearer {stripe_api_key}"},
            )
            logger.info(f"GET {response.url} - Status Code: {response.status_code}")
            if response.status_code != 200:
                logger.error(
                    f"Failed to fetch Stripe charges: {response.status_code} - {response.text}"
                )
                break

            data = response.json()
            fetched_charges = data.get("data", [])
            charges.extend(fetched_charges)
            logger.info(f"Fetched {len(fetched_charges)} Stripe charges.")

            if data.get("has_more"):
                params_charges["starting_after"] = data["data"][-1]["id"]
                logger.info("Fetching next page of Stripe charges.")
            else:
                break
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            logger.error(f"Exception while fetching Stripe charges: {e}")
            break

    # Fetch Refunds
    params_refunds = {"limit": 100, "created[gte]": thirty_days_ago}

    while True:
        try:
            response = session.get(
                base_url_refunds,
                params=params_refunds,
                headers={"Authorization": f"Bearer {stripe_api_key}"},
            )
            logger.info(f"GET {response.url} - Status Code: {response.status_code}")
            if response.status_code != 200:
                logger.error(
                    f"Failed to fetch Stripe refunds: {response.status_code} - {response.text}"
                )
                break

            data = response.json()
            fetched_refunds = data.get("data", [])
            refunds.extend(fetched_refunds)
            logger.info(f"Fetched {len(fetched_refunds)} Stripe refunds.")

            if data.get("has_more"):
                params_refunds["starting_after"] = data["data"][-1]["id"]
                logger.info("Fetching next page of Stripe refunds.")
            else:
                break
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            logger.error(f"Exception while fetching Stripe refunds: {e}")
            break

    logger.info(f"Total Stripe charges fetched: {len(charges)}")
    logger.info(f"Total Stripe refunds fetched: {len(refunds)}")
    return charges, refunds


def store_raw_json(data, filename):
    """
    Stores raw JSON data in the raw_data folder with each record on its own line (JSON Lines format).

    Args:
        data (list): A list of dictionaries representing JSON records.
        filename (str): The name of the file to store the data (without extension).
    """
    file_path = os.path.join(
        "../../data", f"{filename}.jsonl"
    )  # Use .jsonl extension for JSON Lines
    try:
        with open(file_path, "w") as f:
            for record in data:
                json_record = json.dumps(record)
                f.write(json_record + "\n")  # Write each record on a new line
        logger.info(f"Stored data at {file_path}")
    except Exception as e:
        logger.error(f"Failed to store data at {file_path}: {e}")


def main():
    """
    Main function to orchestrate data ingestion from Shopify and Stripe.
    """
    logger.info("Starting data ingestion process.")

    # Create a session with retry logic
    session = create_session()

    # Fetch Shopify Orders
    logger.info("Fetching Shopify orders from the past 30 days.")
    shopify_orders = fetch_shopify_orders(
        session, SHOPIFY_STORE_NAME, SHOPIFY_API_KEY, SHOPIFY_PASSWORD
    )
    if shopify_orders:
        store_raw_json(shopify_orders, "shopify_orders")
    else:
        logger.warning("No Shopify orders fetched.")

    # Fetch Stripe Payments
    logger.info("Fetching Stripe payments from the past 30 days.")
    stripe_charges, stripe_refunds = fetch_stripe_payments(session, STRIPE_API_KEY)
    if stripe_charges:
        store_raw_json(stripe_charges, "stripe_charges")
    else:
        logger.warning("No Stripe charges fetched.")

    if stripe_refunds:
        store_raw_json(stripe_refunds, "stripe_refunds")
    else:
        logger.warning("No Stripe refunds fetched.")

    logger.info("Data ingestion process completed successfully.")


if __name__ == "__main__":
    main()
