import streamlit as st
import requests
import logging
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import pytz

# Streamlit page configuration
st.set_page_config(page_title="ok", layout="wide")

# Initialize session state for scheduled date and time
gmt7 = pytz.timezone('Asia/Bangkok')  # GMT+7 time zone

if 'scheduled_date' not in st.session_state:
    st.session_state.scheduled_date = datetime.now(gmt7).date()

if 'scheduled_time' not in st.session_state:
    st.session_state.scheduled_time = datetime.now(gmt7).time()

# Set up logging to output to stdout
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

app_logger = logging.getLogger('root')
app_logger.setLevel(logging.WARNING)  # Set to WARNING to reduce verbosity

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
app_logger.addHandler(stream_handler)

# Suppress other loggers
logging.getLogger('urllib3').setLevel(logging.WARNING)


# Display current time in GMT+7 for user reference
current_time_gmt7 = datetime.now(gmt7)
st.write(f"**Current Time (GMT+7):** {current_time_gmt7.strftime('%Y-%m-%d %H:%M:%S')}")

# Input text area for orders
input_text = st.text_area("Enter your orders here:", height=200)

# Input for scheduled fulfillment date and time within a form to prevent immediate reruns
with st.form("schedule_form"):
    st.subheader("Schedule Fulfillment Time (GMT+7)")

    # Date input
    scheduled_date_input = st.date_input(
        "Select the fulfillment date:",
        value=st.session_state.scheduled_date
    )

    # Time input
    scheduled_time_input = st.time_input(
        "Select the fulfillment time:",
        value=st.session_state.scheduled_time
    )

    # Submit button for the form
    submit_button = st.form_submit_button(label="Submit Fulfillment Schedule")

if submit_button:
    if not input_text.strip():
        st.warning("Please enter at least one order.")
    else:
        # Combine date and time inputs into a naive datetime object
        scheduled_datetime_input = datetime.combine(scheduled_date_input, scheduled_time_input)

        # Localize the naive datetime to GMT+7
        scheduled_datetime_gmt7 = gmt7.localize(scheduled_datetime_input)

        # Convert to UTC
        scheduled_time_utc = scheduled_datetime_gmt7.astimezone(pytz.utc)

        # Validate that the scheduled time is not in the past
        current_time_gmt7 = datetime.now(gmt7)
        if scheduled_datetime_gmt7 < current_time_gmt7:
            st.error("Scheduled fulfillment time cannot be in the past. Please select a future time.")
        else:
            # Update session state with user inputs
            st.session_state.scheduled_date = scheduled_date_input
            st.session_state.scheduled_time = scheduled_time_input

            # Save orders to database
            DATABASE_URL = os.environ.get('DATABASE_URL')
            if not DATABASE_URL:
                st.error("Database URL not configured.")
            else:
                try:
                    # Use a context manager to ensure the connection is closed properly
                    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
                        with conn.cursor() as cursor:
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

                            # Parse the input text and prepare data for insertion
                            input_lines = input_text.strip().split('\n')
                            orders_data = []
                            for line in input_lines:
                                parts = line.strip().split()
                                if len(parts) != 3:
                                    app_logger.error(f"Invalid input line: {line}")
                                    st.warning(f"Invalid input line skipped: {line}")
                                    continue
                                order_name, tracking_number, carrier = parts

                                orders_data.append((order_name, tracking_number, carrier, scheduled_time_utc))

                            if orders_data:
                                # Insert orders into the database
                                insert_query = """
                                INSERT INTO orders (order_name, tracking_number, carrier, scheduled_time)
                                VALUES %s;
                                """
                                execute_values(cursor, insert_query, orders_data)
                                conn.commit()

                                st.success("Orders have been submitted for fulfillment at the scheduled time. You can close the browser now.")
                            else:
                                st.warning("No valid orders to submit.")
                except psycopg2.DatabaseError as db_error:
                    app_logger.error(f"Database error: {str(db_error)}")
                    st.error("An error occurred while saving orders to the database.")
                except Exception as e:
                    app_logger.error(f"Unexpected error: {str(e)}")
                    st.error("An unexpected error occurred.")
