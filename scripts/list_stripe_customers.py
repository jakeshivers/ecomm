import stripe
import os
from dotenv import load_dotenv

load_dotenv()

# Set your test secret API key
stripe.api_key = os.getenv("STRIPE_API_KEY")

print(str(os.getenv("STRIPE_API_KEY")))

customer = stripe.Customer.create(email="customer@example.com", name="John Doe")
print(customer)


# Example: Retrieve a list of all customers
customers = stripe.Customer.list()

for customer in customers.auto_paging_iter():
    print(f"Customer ID: {customer.id}, Email: {customer.email}")
