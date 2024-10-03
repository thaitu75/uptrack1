import time
import logging
import os
import requests
import psycopg2
import psycopg2.extras
from logging.handlers import RotatingFileHandler

# Set up logging to output to stdout
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

app_logger = logging.getLogger('worker')
app_logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
app_logger.addHandler(stream_handler)

# Suppress other loggers
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Load store configurations from environment variables
def load_stores():
    stores = {}
    for key in os.environ:
        if key.startswith("STORE_") and key.endswith("_ORDER_PREFIX"):
            store_number = key.split("_")[1]
            order_prefix = os.environ[key].upper()
            store_url_key = f"STORE_{store_number}_STORE_URL"
            access_token_key = f"STORE_{store_number}_ACCESS_TOKEN"
            store_url = os.environ.get(store_url_key)
            access_token = os.environ.get(access_token_key)
            if store_url and access_token:
                stores[order_prefix] = {
                    'store_url': store_url,
                    'access_token': access_token,
                    'success_count': 0,
                    'failure_count': 0,
                    'failed_orders': []
                }
            else:
                app_logger.error(f"Missing store URL or access token for store {store_number}")
    return stores

# Function to send Telegram message
def send_telegram_message(message):
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        app_logger.error("Telegram bot token or chat ID not configured.")
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        app_logger.error(f"Failed to send Telegram message: {response.text}")

def process_orders():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        app_logger.error("Database URL not configured.")
        return

    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Load stores
        stores = load_stores()
        store_prefixes = {prefix.upper(): store for prefix, store in stores.items()}

        while True:
            # Fetch pending orders
            cursor.execute("SELECT * FROM orders WHERE status = 'pending';")
            orders = cursor.fetchall()

            if not orders:
                app_logger.info("No pending orders. Sleeping for 30 seconds.")
                time.sleep(30)
                continue

            total_orders = len(orders)
            total_successful = 0
            total_failed = 0

            for order_row in orders:
                order_id = order_row['id']
                order_name = order_row['order_name']
                tracking_number = order_row['tracking_number']
                carrier = order_row['carrier']

                # Determine which store the order belongs to based on the first two characters
                order_prefix = order_name[:2].upper()
                if order_prefix not in store_prefixes:
                    app_logger.error(f"Order {order_name} does not match any configured store prefixes.")
                    cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                    conn.commit()
                    total_failed += 1
                    continue

                store = store_prefixes[order_prefix]
                shop_url = store['store_url']
                access_token = store['access_token']

                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }

                # Initialize rate limiting variables
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
                            # Adjust sleep time dynamically
                            if current_calls / max_calls > 0.8:
                                sleep_time = min(MAX_SLEEP_TIME, MIN_SLEEP_TIME * (current_calls / max_calls) * 5)
                                app_logger.warning(f"Approaching API rate limit. Sleeping for {sleep_time:.2f} seconds.")
                                time.sleep(sleep_time)
                            else:
                                time.sleep(MIN_SLEEP_TIME)
                        else:
                            time.sleep(MIN_SLEEP_TIME)

                        if response.status_code == 429:
                            retry_after = int(response.headers.get('Retry-After', 5))
                            app_logger.warning(f"Received 429 Too Many Requests. Retrying after {retry_after} seconds.")
                            time.sleep(retry_after)
                            continue
                        else:
                            return response

                try:
                    # Step 1: Get order ID by order name
                    response = make_api_call(
                        'GET',
                        f"{shop_url}/admin/api/2023-10/orders.json",
                        params={"name": order_name}
                    )
                    if response.status_code != 200:
                        app_logger.error(f"Error fetching order {order_name}: {response.text}")
                        cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                        conn.commit()
                        store['failure_count'] += 1
                        store['failed_orders'].append(order_name)
                        total_failed += 1
                        continue

                    orders_response = response.json().get('orders', [])
                    if not orders_response:
                        app_logger.error(f"Order {order_name} not found.")
                        cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                        conn.commit()
                        store['failure_count'] += 1
                        store['failed_orders'].append(order_name)
                        total_failed += 1
                        continue

                    order_data = orders_response[0]  # Assuming order name is unique
                    shopify_order_id = order_data['id']

                    # Step 2: Get fulfillment orders for the order ID
                    response = make_api_call(
                        'GET',
                        f"{shop_url}/admin/api/2023-10/orders/{shopify_order_id}/fulfillment_orders.json"
                    )
                    if response.status_code != 200:
                        app_logger.error(f"Error fetching fulfillment orders for {order_name}: {response.text}")
                        cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                        conn.commit()
                        store['failure_count'] += 1
                        store['failed_orders'].append(order_name)
                        total_failed += 1
                        continue

                    fulfillment_orders = response.json().get('fulfillment_orders', [])
                    if not fulfillment_orders:
                        app_logger.error(f"No fulfillment orders found for order {order_name}.")
                        cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                        conn.commit()
                        store['failure_count'] += 1
                        store['failed_orders'].append(order_name)
                        total_failed += 1
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

                    if order_fulfilled:
                        cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('fulfilled', order_id))
                        conn.commit()
                        store['success_count'] += 1
                        total_successful += 1
                        app_logger.info(f"Order {order_name} processing completed with at least one fulfillment.")
                    else:
                        cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                        conn.commit()
                        store['failure_count'] += 1
                        store['failed_orders'].append(order_name)
                        total_failed += 1
                        app_logger.warning(f"No fulfillments were processed for order {order_name}.")

                except Exception as e:
                    app_logger.error(f"An error occurred while processing order {order_name}: {str(e)}")
                    cursor.execute("UPDATE orders SET status = %s WHERE id = %s;", ('failed', order_id))
                    conn.commit()
                    store['failure_count'] += 1
                    store['failed_orders'].append(order_name)
                    total_failed += 1

            # Prepare the summary message
            summary_message = "*Shopify Fulfillment Summary*\n\n"
            summary_message += f"Total orders processed: {total_orders}\n"
            summary_message += f"Successful: {total_successful}\n"
            summary_message += f"Failed: {total_failed}\n\n"

            # Add per-store details
            for prefix, store in stores.items():
                summary_message += f"Store with prefix '{prefix}':\n"
                summary_message += f"- Successful: {store['success_count']}\n"
                summary_message += f"- Failed: {store['failure_count']}\n"
                if store['failed_orders']:
                    summary_message += f"- Orders Failed: {', '.join(store['failed_orders'])}\n"
                summary_message += "\n"

            # Send summary via Telegram
            send_telegram_message(summary_message)

    except Exception as e:
        app_logger.error(f"Database error: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    process_orders()
