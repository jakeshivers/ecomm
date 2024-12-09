import shopify
import os
import logging
import random
import time
from dotenv import load_dotenv

load_dotenv()

# Shopify credentials from environment
SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")  # e.g. "capstone-data"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")  # Admin API token

# Configuration for rate limiting and retries
REQUEST_DELAY = 2  # seconds between requests to reduce rate limit issues
RETRY_WAIT = 60  # seconds to wait if rate limit (429) is encountered
MAX_RETRIES = 2  # max retries if hitting rate limits


def initialize_shopify_session():
    try:
        shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
        session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
        shopify.ShopifyResource.activate_session(session)
        print("Shopify session initialized successfully.")
        logging.info("Shopify session initialized successfully.")
    except Exception as e:
        print(f"Error initializing Shopify session: {e}")
        logging.error(f"Error initializing Shopify session: {e}")


def handle_rate_limit(func, *args, **kwargs):
    """
    A helper function to handle Shopify API calls and retry on 429 errors.
    """
    for attempt in range(MAX_RETRIES):
        try:
            result = func(*args, **kwargs)
            time.sleep(REQUEST_DELAY)  # Wait after a successful request
            return result
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Exceeded draft order API rate limit" in msg:
                logging.warning(
                    f"Rate limit hit. Waiting {RETRY_WAIT} seconds before retry..."
                )
                print(f"Rate limit hit. Waiting {RETRY_WAIT} seconds before retry...")
                time.sleep(RETRY_WAIT)
            else:
                logging.error(f"API error: {e}")
                raise
    logging.error("Max retries reached after hitting rate limits.")
    print("Max retries reached after hitting rate limits.")
    return None


def select_random_customer():
    print("Selecting a random customer...")
    logging.info("Selecting a random customer...")
    customers = handle_rate_limit(shopify.Customer.find, limit=250)
    if not customers:
        print("No customers found.")
        logging.warning("No customers found.")
        return None
    customer = random.choice(customers)
    print(f"Selected customer {customer.id} - {customer.email}")
    logging.info(f"Selected customer {customer.id} - {customer.email}")
    return customer


def select_random_products(num_items=3):
    print("Selecting random products...")
    logging.info("Selecting random products...")
    products = handle_rate_limit(shopify.Product.find, limit=250)
    if not products:
        print("No products found.")
        logging.warning("No products found.")
        return []

    # Choose a unique set of products between 1 and 3 items
    count = random.randint(1, num_items)
    if len(products) < count:
        count = len(products)

    unique_products = random.sample(products, count)

    chosen_items = []
    for product in unique_products:
        if not product.variants:
            continue
        variant = random.choice(product.variants)
        quantity = random.randint(1, 3)
        chosen_items.append({"variant_id": variant.id, "quantity": quantity})
        print(f"Added {quantity} x Variant {variant.id} of {product.title}")
        logging.info(f"Added {quantity} x Variant {variant.id} of {product.title}")

    return chosen_items


def create_draft_order(customer, line_items):
    try:
        print("Creating draft order...")
        logging.info("Creating draft order...")
        draft_order = shopify.DraftOrder()
        draft_order.line_items = line_items
        # Attach the random customer by ID and email
        draft_order.customer = {"id": customer.id}
        draft_order.email = customer.email
        draft_order.use_customer_default_address = True
        draft_order.financial_status = "pending"
        success = handle_rate_limit(draft_order.save)
        if not success or not draft_order.id:
            logging.error("Draft order creation failed.")
            print("Draft order creation failed.")
            return None
        print(f"Draft order {draft_order.id} created with status {draft_order.status}")
        logging.info(
            f"Draft order {draft_order.id} created with status {draft_order.status}"
        )
        return draft_order
    except Exception as e:
        print(f"Error creating draft order: {e}")
        logging.error(f"Error creating draft order: {e}")
        return None


def complete_and_fulfill_draft_order(draft_order):
    try:
        # Mark as paid
        print(f"Marking draft order {draft_order.id} as paid...")
        logging.info(f"Marking draft order {draft_order.id} as paid...")
        draft_order.financial_status = "paid"
        handle_rate_limit(draft_order.save)

        print(f"Completing draft order {draft_order.id}...")
        logging.info(f"Completing draft order {draft_order.id}...")
        handle_rate_limit(draft_order.complete)
        print(f"Draft order {draft_order.id} completed.")
        logging.info(f"Draft order {draft_order.id} completed.")

        # Fetch completed order
        completed_order = handle_rate_limit(shopify.Order.find, draft_order.order_id)
        if not completed_order:
            logging.error("Failed to fetch completed order.")
            print("Failed to fetch completed order.")
            return
        print(f"Fetched completed order {completed_order.id}")
        logging.info(f"Fetched completed order {completed_order.id}")

        # Fulfill the order
        fulfill_order(completed_order)

    except Exception as e:
        print(f"Error completing/fulfilling draft order {draft_order.id}: {e}")
        logging.error(f"Error completing/fulfilling draft order {draft_order.id}: {e}")


def fulfill_order(completed_order):
    try:
        locations = handle_rate_limit(shopify.Location.find)
        if not locations:
            print("No locations found for fulfillment.")
            logging.error("No locations found for fulfillment.")
            return
        location_id = locations[0].id
        print(f"Using location_id: {location_id}")
        logging.info(f"Using location_id: {location_id}")

        fulfillment_line_items = [
            {"id": li.id, "quantity": li.quantity} for li in completed_order.line_items
        ]
        print("Line items for fulfillment:", fulfillment_line_items)
        logging.info(f"Line items for fulfillment: {fulfillment_line_items}")

        fulfillment_data = {
            "fulfillment": {
                "order_id": completed_order.id,
                "location_id": location_id,
                "line_items": fulfillment_line_items,
            }
        }

        print(f"Fulfilling order {completed_order.id}...")
        logging.info(f"Fulfilling order {completed_order.id}...")
        fulfillment = handle_rate_limit(shopify.Fulfillment.create, fulfillment_data)

        if fulfillment and not fulfillment.errors:
            print(f"Order {completed_order.id} fulfilled successfully.")
            logging.info(f"Order {completed_order.id} fulfilled successfully.")
        else:
            errors = (
                fulfillment.errors.full_messages()
                if fulfillment and fulfillment.errors
                else "Unknown error"
            )
            print(f"Error fulfilling order {completed_order.id}: {errors}")
            logging.error(f"Error fulfilling order {completed_order.id}: {errors}")
    except Exception as e:
        print(f"Error fulfilling order {completed_order.id}: {e}")
        logging.error(f"Error fulfilling order {completed_order.id}: {e}")


def main():
    log_folder = "log"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_file = os.path.join(log_folder, "shopify_random_order_fulfillment.log")
    # Reset basicConfig to log to file
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    initialize_shopify_session()

    # Select random customer
    customer = select_random_customer()
    if not customer:
        print("No customer selected. Exiting.")
        return

    # Select a unique set of products (1 to 3)
    line_items = select_random_products(num_items=3)
    if not line_items:
        print("No line items selected. Exiting.")
        logging.warning("No line items selected.")
        return

    # Create draft order with the customer attached
    draft_order = create_draft_order(customer, line_items)
    if not draft_order:
        print("Failed to create draft order. Exiting.")
        return

    # Complete and fulfill draft order
    complete_and_fulfill_draft_order(draft_order)

    print("Process complete.")
    logging.info("Process complete.")


if __name__ == "__main__":
    main()
