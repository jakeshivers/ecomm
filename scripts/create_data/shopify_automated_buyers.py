import os
import random
import logging
from dotenv import load_dotenv
import shopify

# Load environment variables
load_dotenv()

SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")  # e.g., "capstone-data"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")  # Admin API token


# Determine the directory for the logs folder, one level up
# Create log directory if it doesn't exist
log_dir = "../logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Path to the log file
log_file_path = os.path.join(log_dir, "shopify_script.log")

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


def initialize_shopify_session():
    try:
        shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
        session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
        shopify.ShopifyResource.activate_session(session)
        logger.info("Shopify session initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing Shopify session: {e}")
        raise


def create_random_order():
    try:
        logger.info("Selecting a random customer...")
        customers = shopify.Customer.find(limit=250)
        if not customers:
            logger.warning("No customers found.")
            return None
        customer = random.choice(customers)
        logger.info(f"Selected customer {customer.id} ({customer.email})")

        logger.info("Selecting random products...")
        products = shopify.Product.find(limit=250)
        if not products:
            logger.warning("No products found.")
            return None
        random_items = []
        num_items = random.randint(1, 3)
        for _ in range(num_items):
            product = random.choice(products)
            if not product.variants:
                continue
            variant = random.choice(product.variants)
            qty = random.randint(1, 3)
            random_items.append({"variant_id": variant.id, "quantity": qty})
            logger.info(f"Added {qty} x {product.title} (variant {variant.id})")

        if not random_items:
            logger.warning("No line items selected. Exiting.")
            return None

        logger.info("Creating draft order...")
        draft_order = shopify.DraftOrder()
        draft_order.line_items = [
            {"variant_id": item["variant_id"], "quantity": item["quantity"]}
            for item in random_items
        ]
        draft_order.customer = {"id": customer.id}
        draft_order.email = customer.email
        draft_order.use_customer_default_address = True
        draft_order.financial_status = "pending"
        if not draft_order.save():
            logger.error("Failed to create draft order.")
            return None
        logger.info(f"Draft order {draft_order.id} created.")

        logger.info(f"Marking draft order {draft_order.id} as paid...")
        draft_order.financial_status = "paid"
        if not draft_order.save():
            logger.error("Failed to mark draft order as paid.")
            return None
        logger.info("Draft order marked as paid.")

        logger.info(f"Completing draft order {draft_order.id}...")
        draft_order.complete()

        completed_order = shopify.Order.find(draft_order.order_id)
        if not completed_order:
            logger.error("Failed to fetch completed order.")
            return None
        logger.info(f"Order {completed_order.id} created and completed successfully.")
        return completed_order
    except Exception as e:
        logger.error(f"Error creating random order: {e}")
        return None


def main():
    try:
        initialize_shopify_session()

        order = create_random_order()
        if not order:
            return

        # Log order ID and note that it is left unfulfilled
        logger.info(f"Order {order.id} created and left as unfulfilled.")
    except Exception as e:
        logger.error(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
