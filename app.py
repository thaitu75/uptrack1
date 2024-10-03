import streamlit as st
import requests
import time
import logging
import os
import psycopg2
from psycopg2.extras import execute_values
from logging.handlers import RotatingFileHandler

# Streamlit page configuration
st.set_page_config(page_title="Shopify Multi-Store Bulk Fulfillment Tool", layout="wide")

# Set up logging to output to stdout
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

app_logger = logging.getLogger('root')
app_logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
app_logger.addHandler(stream_handler)

# Suppress other loggers
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Set up the title and description
st.title("Shopify Multi-Store Bulk Fulfillment Tool")
st.write("""
This tool allows you to fulfill multiple Shopify orders across multiple stores at once by entering the order information below.

Please input the orders in the following format (one per line):

`OrderName    TrackingNumber    Carrier`

**Example:**

""")

# Input text area for orders
input_text = st.text_area("Enter your orders here:", height=200)

# Button to start processing
if st.button("Fulfill Orders"):
    if not input_text.strip():
        st.warning("Please enter at least one order.")
    else:
        # Save orders to database
        try:
            DATABASE_URL = os.environ.get('DATABASE_URL')
            if not DATABASE_URL:
                st.error("Database URL not configured.")
            else:
                # Connect to the database
                conn = psycopg2.connect(DATABASE_URL, sslmode='require')
                cursor = conn.cursor()

                # Create orders table if it doesn't exist
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    order_name TEXT NOT NULL,
                    tracking_number TEXT NOT NULL,
                    carrier TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                conn.commit()

                # Parse the input text and prepare data for insertion
                input_lines = input_text.strip().split('\n')
                orders_data = []
                for line in input_lines:
                    parts = line.strip().split()
                    if len(parts) != 3:
                        app_logger.error(f"Invalid input line: {line}")
                        continue
                    order_name, tracking_number, carrier = parts
                    orders_data.append((order_name, tracking_number, carrier))

                # Insert orders into the database
                insert_query = """
                INSERT INTO orders (order_name, tracking_number, carrier)
                VALUES %s;
                """
                execute_values(cursor, insert_query, orders_data)
                conn.commit()

                cursor.close()
                conn.close()

                st.success("Orders have been submitted for fulfillment. You can close the browser now.")
        except Exception as e:
            app_logger.error(f"Database error: {str(e)}")
            st.error("An error occurred while saving orders to the database.")
