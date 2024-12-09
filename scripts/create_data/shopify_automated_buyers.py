import shopify
import random
from dotenv import load_dotenv
import datetime
import json
import os
import time

load_dotenv()

SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")  # e.g. "capstone-data"
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")  # Admin API token


def initialize_shopify_session():
    shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
    session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
    shopify.ShopifyResource.activate_session(session)
    print("Shopify session initialized successfully.")


def create_random_order():
    # Select a random customer
    print("Selecting a random customer...")
    customers = shopify.Customer.find(limit=250)
    if not customers:
        print("No customers found.")
        return None
    customer = random.choice(customers)
    print(f"Selected customer {customer.id} ({customer.email})")

    # Select random products
    print("Selecting random products...")
    products = shopify.Product.find(limit=250)
    if not products:
        print("No products found.")
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
        print(f"Added {qty} x {product.title} (variant {variant.id})")

    if not random_items:
        print("No line items selected. Exiting.")
        return None

    # Create a draft order
    print("Creating draft order...")
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
        print("Failed to create draft order.")
        return None
    print(f"Draft order {draft_order.id} created.")

    # Mark draft order as paid
    print(f"Marking draft order {draft_order.id} as paid...")
    draft_order.financial_status = "paid"
    if not draft_order.save():
        print("Failed to mark draft order as paid.")
        return None
    print("Draft order marked as paid.")

    # Complete the draft order to create a real order
    print(f"Completing draft order {draft_order.id}...")
    draft_order.complete()
    # Fetch the completed order
    completed_order = shopify.Order.find(draft_order.order_id)
    if not completed_order:
        print("Failed to fetch completed order.")
        return None
    print(f"Order {completed_order.id} created and completed successfully.")

    return completed_order


def fulfill_order(order):
    # Fulfill the order by fulfilling each fulfillable line item
    print(f"Fulfilling order {order.id}...")
    fulfillment_line_items = []
    for li in order.line_items:
        fulfillable_qty = getattr(li, "fulfillable_quantity", 0)
        if li.fulfillment_service == "manual" and fulfillable_qty > 0:
            fulfillment_line_items.append({"id": li.id, "quantity": fulfillable_qty})

    if not fulfillment_line_items:
        print("No items to fulfill.")
        return False

    fulfillment = shopify.Fulfillment()
    fulfillment.order_id = order.id
    fulfillment.line_items = fulfillment_line_items
    # Add fake tracking
    fulfillment.notify_customer = False
    fulfillment.tracking_number = "FAKE123456789"
    fulfillment.tracking_company = "Fake Carrier"

    if fulfillment.save():
        if fulfillment.errors:
            print("Error fulfilling order:", fulfillment.errors.full_messages())
            return False
        else:
            print(f"Order {order.id} fulfilled successfully.")
            return True
    else:
        print("Error creating fulfillment.")
        return False


def close_order(order):
    # Close the order
    print(f"Closing order {order.id}...")
    path = f"orders/{order.id}/close.json"
    resp = shopify.ShopifyResource.connection.post(path)
    if resp.code == 200:
        print(f"Order {order.id} closed successfully.")
    else:
        print(f"Failed to close order {order.id}. Response code: {resp.code}")


def main():
    initialize_shopify_session()

    order = create_random_order()
    if not order:
        return

    # Fulfill the order if possible
    if fulfill_order(order):
        close_order(order)


if __name__ == "__main__":
    main()
