import streamlit as st
import requests
import time
import logging
import os
import psycopg2
from psycopg2.extras import execute_values
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta

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

# Input for scheduled fulfillment date and time
scheduled_date_input = st.date_input("Select the fulfillment date (GMT+7):", value=(datetime.now() + timedelta(hours=7)).date())
scheduled_time_input = st.time_input("Select the fulfillment time (GMT+7):", value=(datetime.now() + timedelta(hours=7)).time())

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
                    scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """)
                conn.commit()

                # Combine date and time inputs into a datetime object
                scheduled_datetime_input = datetime.combine(scheduled_date_input, scheduled_time_input)
                # Assign GMT+7 timezone to the datetime
                scheduled_datetime_gmt7 = scheduled_datetime_input.replace(tzinfo=timezone(timedelta(hours=7)))
                # Convert to UTC
                scheduled_time_utc = scheduled_datetime_gmt7.astimezone(timezone.utc)

                # Parse the input text and prepare data for insertion
                input_lines = input_text.strip().split('\n')
                orders_data = []
                for line in input_lines:
                    parts = line.strip().split()
                    if len(parts) != 3:
                        app_logger.error(f"Invalid input line: {line}")
                        continue
                    order_name, tracking_number, carrier = parts

                    orders_data.append((order_name, tracking_number, carrier, scheduled_time_utc))

                # Insert orders into the database
                insert_query = """
                INSERT INTO orders (order_name, tracking_number, carrier, scheduled_time)
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
