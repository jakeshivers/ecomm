from pyspark.sql import SparkSession
import requests
import json
import os

# Initialize Spark session
spark = SparkSession.builder.appName("ShopifyOrderData").getOrCreate()

# Shopify API configuration
SHOPIFY_API_KEY = "your_shopify_api_key"
SHOPIFY_PASSWORD = "your_shopify_password"
SHOPIFY_STORE_NAME = "your_shopify_store_name"
SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_PASSWORD}@{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2023-10/orders.json"

# S3 raw data lake folder configuration
RAW_DATA_LAKE_PATH = "s3://your-bucket-name/raw/shopify/orders/"


# Function to fetch Shopify order data using API
def fetch_shopify_orders():
    response = requests.get(SHOPIFY_API_URL)
    if response.status_code == 200:
        return response.json()["orders"]
    else:
        raise Exception(
            f"Failed to fetch Shopify orders. Status code: {response.status_code}"
        )


# Function to write data to raw data lake folder
def write_to_data_lake(data, output_path):
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
    df.write.mode("append").json(output_path)


# Fetch the order data from Shopify
try:
    print("Fetching Shopify orders...")
    orders_data = fetch_shopify_orders()
    print(f"Fetched {len(orders_data)} orders")

    # Store raw JSON in S3 (Data Lake folder)
    print(f"Storing raw data to: {RAW_DATA_LAKE_PATH}")
    write_to_data_lake(orders_data, RAW_DATA_LAKE_PATH)

    print("Data successfully written to raw data lake.")

except Exception as e:
    print(f"Error: {e}")
