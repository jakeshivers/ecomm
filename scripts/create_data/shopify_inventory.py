import os
import logging
import time
import json
from dotenv import load_dotenv
from itertools import islice
from pyactiveresource.connection import Error
import shopify

# Load environment variables from .env file
load_dotenv()

# Shopify credentials
SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")  # e.g., "capstone-data"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")  # Admin API token

# Configure logging
log_folder = "logs"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_file = os.path.join(log_folder, "inventory_ingestion.log")

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


def initialize_shopify_session():
    """Initialize Shopify session."""
    try:
        shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
        session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
        shopify.ShopifyResource.activate_session(session)
        logger.info("Shopify session initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing Shopify session: {e}", exc_info=True)
        raise


def chunked_iterable(iterable, size):
    """Helper function to split an iterable into chunks."""
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


def fetch_products_with_inventory():
    """Fetch products with their corresponding inventory levels."""
    try:
        logger.info("Fetching products and inventory levels...")

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
            logger.info(f"Fetched inventory levels for {len(inventory_levels)} items.")

        # Step 3: Map inventory levels back to products
        for inventory_level in all_inventory_levels:
            item_id = inventory_level.inventory_item_id
            if item_id in inventory_item_map:
                inventory_item_map[item_id]["location_id"] = inventory_level.location_id
                inventory_item_map[item_id]["available"] = inventory_level.available

        # Step 4: Format and log the result
        results = []

        for item in inventory_item_map.values():
            results.append(item)
            logger.info(
                f"Timestamp: {item['timestamp']} | "
                f"Product: {item['product_title']} | Variant: {item['variant_title']} | "
                f"SKU: {item['sku']} | Location ID: {item.get('location_id', 'N/A')} | "
                f"Available: {item.get('available', 'N/A')}"
            )

        # Write each record as its own JSON line
        data_file_path = os.path.join(
            "../../data", "shopify_inventory_and_products.jsonl"
        )
        try:
            with open(data_file_path, "w") as f:
                for record in results:
                    f.write(json.dumps(record) + "\n")
            logger.info(f"Stored inventory data at {data_file_path}")
        except Exception as e:
            logger.error(f"Failed to store data at {data_file_path}: {e}")

        return results  # Return for further processing if needed

    except Error as ve:
        logger.error(f"Shopify API validation error: {ve}", exc_info=True)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred in fetch_products_with_inventory(): {e}",
            exc_info=True,
        )
        return []


def main():
    """Main execution function."""
    try:
        logger.info("Starting inventory ingestion process.")

        # Initialize Shopify session
        initialize_shopify_session()

        # Test fetching shop info
        shop = shopify.Shop.current()
        logger.info(f"Connected to Shopify store: {shop.name}")

        # Fetch products with inventory
        fetch_products_with_inventory()

        logger.info("Inventory ingestion process completed successfully.")

    except Exception as e:
        logger.error(f"An unexpected error occurred in main(): {e}", exc_info=True)


if __name__ == "__main__":
    main()
