import os
import shopify
import logging
import time
import json
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_PASSWORD")
ORDER_ID = 6242404663580  # Replace with the order ID you want to fulfill and close

# Set up logging: info only, no HTML or raw body logs
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def initialize_shopify_session():
    shop_url = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10"
    session = shopify.Session(shop_url, "2023-10", SHOPIFY_ACCESS_TOKEN)
    shopify.ShopifyResource.activate_session(session)
    logging.info("Shopify session initialized successfully.")


def handle_api_call(func, *args, **kwargs):
    # Simple wrapper to handle rate limits (2 attempts)
    for attempt in range(2):
        try:
            result = func(*args, **kwargs)
            time.sleep(1)
            return result
        except Exception as e:
            msg = str(e)
            logging.error(f"API call error on attempt {attempt+1}: {msg}")
            if "429" in msg:
                logging.info("Rate limit hit, waiting 60 seconds before retry.")
                time.sleep(60)
            else:
                return None
    return None


def get_fulfillment_orders(order_id):
    # Include the admin path:
    path = f"admin/api/2023-10/orders/{order_id}/fulfillment_orders.json"
    logging.info(f"Fetching fulfillment orders for order {order_id} at path: {path}")
    resp = handle_api_call(shopify.ShopifyResource.connection.get, path)
    if resp and resp.code == 200:
        data = json.loads(resp.body.decode("utf-8"))
        fos = data.get("fulfillment_orders", [])
        logging.info(f"Found {len(fos)} fulfillment order(s) for order {order_id}.")
        return fos
    else:
        code = resp.code if resp else "No response"
        logging.error(f"Failed to fetch fulfillment orders. Code: {code}")
        return []


def accept_fulfillment_request(fo_id):
    path = f"fulfillment_orders/{fo_id}/fulfillment_request/accept.json"
    payload = {"fulfillment_request": {"message": "Accepting request"}}
    logging.info(f"Accepting fulfillment request for FO {fo_id}")
    resp = handle_api_call(shopify.ShopifyResource.connection.post, path, payload)
    if resp and resp.code == 200:
        logging.info(f"Fulfillment request accepted for FO {fo_id}")
        return True
    else:
        code = resp.code if resp else "No response"
        logging.error(
            f"Failed to accept fulfillment request for FO {fo_id}. Code: {code}"
        )
        return False


def create_fulfillment(fo_id, fo_line_items):
    path = "fulfillments.json"
    payload = {
        "fulfillment": {
            "message": "Fulfilling via FO API",
            "notify_customer": False,
            "tracking_info": {"number": "FAKE123456789", "company": "Fake Carrier"},
            "line_items_by_fulfillment_order": [
                {
                    "fulfillment_order_id": fo_id,
                    "fulfillment_order_line_items": fo_line_items,
                }
            ],
        }
    }
    logging.info(f"Creating fulfillment for FO {fo_id}")
    resp = handle_api_call(shopify.ShopifyResource.connection.post, path, payload)
    if resp and resp.code in [200, 201]:
        data = json.loads(resp.body.decode("utf-8"))
        if "errors" in data:
            logging.error(f"Fulfillment error: {data['errors']}")
            return False
        logging.info(f"Fulfillment created successfully for FO {fo_id}")
        return True
    else:
        code = resp.code if resp else "No response"
        logging.error(f"Failed to create fulfillment for FO {fo_id}. Code: {code}")
        return False


def close_order(order_id):
    path = f"orders/{order_id}/close.json"
    logging.info(f"Closing order {order_id}")
    resp = handle_api_call(shopify.ShopifyResource.connection.post, path)
    if resp and resp.code == 200:
        logging.info(f"Order {order_id} closed successfully.")
    else:
        code = resp.code if resp else "No response"
        logging.error(f"Failed to close order {order_id}. Code: {code}")


def main():
    initialize_shopify_session()

    logging.info(f"Fetching order {ORDER_ID}")
    order = handle_api_call(shopify.Order.find, ORDER_ID)
    if not order:
        logging.error(f"Order {ORDER_ID} not found or no response.")
        return

    logging.info(
        f"Order {ORDER_ID} fetched: financial_status={order.financial_status}, fulfillment_status={order.fulfillment_status}"
    )

    # Get fulfillment orders
    fos = get_fulfillment_orders(ORDER_ID)
    if not fos:
        return

    # Process each FO
    for fo in fos:
        fo_id = fo["id"]
        request_status = fo["request_status"]
        logging.info(f"Processing FO {fo_id}, request_status={request_status}")

        if request_status == "unsubmitted":
            if not accept_fulfillment_request(fo_id):
                return

        # Gather FO line items that have > 0 quantity
        fo_line_items = []
        for foi_li in fo["line_items"]:
            if foi_li["quantity"] > 0:
                fo_line_items.append(
                    {"id": foi_li["id"], "quantity": foi_li["quantity"]}
                )

        if not fo_line_items:
            logging.info(f"No line items to fulfill in FO {fo_id}")
            continue

        # Create the fulfillment
        if not create_fulfillment(fo_id, fo_line_items):
            return

    # After fulfilling, try closing the order
    close_order(ORDER_ID)


if __name__ == "__main__":
    main()
