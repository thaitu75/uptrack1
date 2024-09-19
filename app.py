import streamlit as st
import requests
import time
import logging
import os
from logging.handlers import RotatingFileHandler

# Streamlit page configuration
st.set_page_config(page_title="Shopify Multi-Store Bulk Fulfillment Tool", layout="wide")

# Set up logging with rotation
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = 'app.log'

my_handler = RotatingFileHandler(log_file, mode='a', maxBytes=5*1024*1024, backupCount=2)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_logger = logging.getLogger('root')
app_logger.setLevel(logging.INFO)
app_logger.addHandler(my_handler)

# Suppress other loggers
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Set up the title and description
st.title("Shopify Multi-Store Bulk Fulfillment Tool")
st.write("""
This tool allows you to fulfill multiple Shopify orders across multiple stores at once by entering the order information below.
Please input the orders in the following format (one per line):

`OrderName    TrackingNumber    Carrier`

Example:

G12345 00340434518424504153 DHL C54321 00340434518424554349 DHL U67890 00340434518424567890 DHL

""")

# Input text area for orders
input_text = st.text_area("Enter your orders here:", height=200)

# Button to start processing
if st.button("Fulfill Orders"):
    if not input_text.strip():
        st.warning("Please enter at least one order.")
    else:
        # Show a progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()

        # Load store configurations from secrets.toml
        stores = {}
        # Load store configurations from environment variables
stores = {}
store_number = 1
while True:
    store_key = f"STORE_{store_number}"
    store_config_str = os.getenv(store_key)
    if not store_config_str:
        break  # No more stores
    try:
        # Expected format: order_prefix,store_url,access_token
        order_prefix, store_url, access_token = store_config_str.split(',', 2)
        stores[order_prefix.upper()] = {
            'store_url': store_url,
            'access_token': access_token,
            'success_count': 0,
            'failure_count': 0,
            'not_found_orders': [],
            'failed_orders': []
        }
    except ValueError:
        app_logger.error(f"Invalid format for {store_key}. Expected 'order_prefix,store_url,access_token'")
    store_number += 1

        # Prepare store prefixes for case-insensitive comparison
        store_prefixes = {prefix.upper(): store for prefix, store in stores.items()}

        # Parse the input text
        input_lines = input_text.strip().split('\n')
        total_orders = len(input_lines)
        processed_orders = 0

        # Initialize rate limiting variables
        MAX_CALLS = 40  # Shopify API bucket size
        CALL_LEAK_RATE = 2  # Shopify API leak rate per second
        MIN_SLEEP_TIME = 0.5  # Minimum sleep time in seconds
        MAX_SLEEP_TIME = 5  # Maximum sleep time in seconds

        # Create a session object to reuse TCP connections
        session = requests.Session()

        # Function to make API calls with rate limit handling
        def make_api_call(method, url, **kwargs):
            while True:
                response = session.request(method, url, headers=headers, **kwargs)
                api_call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit')
                if api_call_limit:
                    current_calls, max_calls = map(int, api_call_limit.split('/'))
                    app_logger.info(f"API Call Limit: {current_calls}/{max_calls}")
                    # If we're close to the limit, sleep longer
                    if current_calls / max_calls > 0.8:
                        sleep_time = min(MAX_SLEEP_TIME, MIN_SLEEP_TIME * (current_calls / max_calls) * 5)
                        app_logger.warning(f"Approaching API rate limit. Sleeping for {sleep_time:.2f} seconds.")
                        time.sleep(sleep_time)
                    else:
                        time.sleep(MIN_SLEEP_TIME)
                else:
                    # If header is missing, default sleep
                    time.sleep(MIN_SLEEP_TIME)

                if response.status_code == 429:
                    # Too Many Requests, implement exponential backoff
                    retry_after = int(response.headers.get('Retry-After', 5))
                    app_logger.warning(f"Received 429 Too Many Requests. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue  # Retry the request
                else:
                    return response

        try:
            for line in input_lines:
                # Update progress bar
                processed_orders += 1
                progress = processed_orders / total_orders
                progress_bar.progress(progress)

                # Parse the input line
                parts = line.strip().split()
                if len(parts) != 3:
                    app_logger.error(f"Invalid input line: {line}")
                    continue
                order_name, tracking_number, carrier = parts

                # Determine which store the order belongs to based on the prefix
                order_prefix = order_name[0].upper()
                if order_prefix not in store_prefixes:
                    app_logger.error(f"Order {order_name} does not match any configured store prefixes.")
                    continue

                store = store_prefixes[order_prefix]
                shop_url = store['store_url']
                access_token = store['access_token']

                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }

                # Step 1: Get order ID by order name
                response = make_api_call(
                    'GET',
                    f"{shop_url}/admin/api/2023-10/orders.json",
                    params={"name": order_name}
                )
                if response.status_code != 200:
                    app_logger.error(f"Error fetching order {order_name}: {response.text}")
                    store['failed_orders'].append(order_name)
                    store['failure_count'] += 1
                    continue

                orders = response.json().get('orders', [])
                if not orders:
                    app_logger.error(f"Order {order_name} not found.")
                    store['not_found_orders'].append(order_name)
                    store['failure_count'] += 1
                    continue

                order = orders[0]  # Assuming order name is unique
                order_id = order['id']

                # Step 2: Get fulfillment orders for the order ID
                response = make_api_call(
                    'GET',
                    f"{shop_url}/admin/api/2023-10/orders/{order_id}/fulfillment_orders.json"
                )
                if response.status_code != 200:
                    app_logger.error(f"Error fetching fulfillment orders for {order_name}: {response.text}")
                    store['failed_orders'].append(order_name)
                    store['failure_count'] += 1
                    continue

                fulfillment_orders = response.json().get('fulfillment_orders', [])
                if not fulfillment_orders:
                    app_logger.error(f"No fulfillment orders found for order {order_name}.")
                    store['failed_orders'].append(order_name)
                    store['failure_count'] += 1
                    continue

                # Initialize a flag to track if any fulfillment was successful
                order_fulfilled = False

                # Step 3: Create fulfillments with tracking information
                for fo in fulfillment_orders:
                    fulfillment_order_id = fo['id']
                    fulfillment_order_status = fo['status']

                    # Check if the fulfillment order is fulfillable
                    if fulfillment_order_status != 'open':
                        app_logger.warning(f"Cannot fulfill fulfillment order {fulfillment_order_id} for order {order_name}: status is '{fulfillment_order_status}'.")
                        continue

                    tracking_url = f"{shop_url}/apps/trackingmore?nums={tracking_number}"

                    fulfillment_data = {
                        "fulfillment": {
                            # "message": "Your order has been shipped!",  # Optional
                            "notify_customer": False,
                            "tracking_info": {
                                "company": carrier,
                                "number": tracking_number,
                                "url": tracking_url
                            },
                            "line_items_by_fulfillment_order": [
                                {
                                    "fulfillment_order_id": fulfillment_order_id
                                }
                            ]
                        }
                    }

                    response = make_api_call(
                        'POST',
                        f"{shop_url}/admin/api/2023-10/fulfillments.json",
                        json=fulfillment_data
                    )

                    if response.status_code == 201:
                        app_logger.info(f"Fulfillment order {fulfillment_order_id} for order {order_name} fulfilled successfully.")
                        order_fulfilled = True
                    else:
                        app_logger.error(f"Failed to fulfill fulfillment order {fulfillment_order_id} for order {order_name}: {response.text}")
                        store['failed_orders'].append(order_name)
                        store['failure_count'] += 1

                if order_fulfilled:
                    store['success_count'] += 1
                    app_logger.info(f"Order {order_name} processing completed with at least one fulfillment.")
                else:
                    if order_name not in store['failed_orders']:
                        store['failed_orders'].append(order_name)
                        store['failure_count'] += 1
                    app_logger.warning(f"No fulfillments were processed for order {order_name}.")

        except Exception as e:
            app_logger.error(f"An error occurred: {str(e)}")

        # Indicate processing is complete
        st.success("All orders have been processed.")

        # Display summary per store
        st.header("Summary")
        for prefix, store in stores.items():
            st.subheader(f"Store with prefix '{prefix}'")
            st.write(f"Store URL: {store['store_url']}")
            st.write(f"Successful fulfillments: {store['success_count']}")
            st.write(f"Failed fulfillments: {store['failure_count']}")

            if store['not_found_orders']:
                st.write("Orders not found:")
                for order_name in store['not_found_orders']:
                    st.write(f"- {order_name}")

            if store['failed_orders']:
                st.write("Orders that failed to fulfill:")
                for order_name in set(store['failed_orders']):
                    st.write(f"- {order_name}")

            st.write("---")
