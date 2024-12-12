import logging
import os

import stripe_create_customers
import shopify_automated_buyers
import shopify_fulfill_existing_orders
import shopify_inventory

# Configure logging
log_file_path = os.path.abspath("orchestration.log")
print(f"Logging to: {log_file_path}")  # Debug the log file path

# Configure logging handlers
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])
logger = logging.getLogger(__name__)


# Main entry point for the script
def main():
    try:
        logger.info("Starting main script orchestration.")

        # Initialize Shopify session
        logger.info("Starting stripe_create_customers...")
        stripe_create_customers.main()

        logger.info("Starting shopify_automated_buyers...")
        shopify_automated_buyers.main()

        logger.info("Starting shopify_fulfill_existing_orders...")
        shopify_fulfill_existing_orders.main()

        logger.info("Starting shopify_inventory...")
        shopify_inventory.main()

        logger.info("Main script orchestration completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during script orchestration: {e}")
        raise


if __name__ == "__main__":
    main()
