import streamlit as st
import requests
import logging
import os
import psycopg2
from psycopg2.extras import execute_values, DictCursor
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

# --- Begin Sidebar Code ---

# Function to fetch scheduled orders from the database
def fetch_scheduled_orders():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        st.sidebar.error("Database URL not configured.")
        return []

    try:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("""
                SELECT scheduled_time, status, COUNT(*) as order_count
                FROM orders
                GROUP BY scheduled_time, status
                ORDER BY scheduled_time DESC;
                """)
                orders = cursor.fetchall()
                return orders
    except Exception as e:
        app_logger.error(f"Database error while fetching scheduled orders: {str(e)}")
        st.sidebar.error("Error fetching scheduled orders.")
        return []

# Display scheduled orders in the sidebar
def display_scheduled_orders():
    st.sidebar.title("Scheduled Orders")

    orders = fetch_scheduled_orders()
    if not orders:
        st.sidebar.info("No scheduled orders found.")
        return

    # Prepare a summary dictionary
    summary = {}
    for order in orders:
        scheduled_time_utc = order['scheduled_time']
        status = order['status']
        order_count = order['order_count']

        # Convert scheduled_time to GMT+7
        scheduled_time_gmt7 = scheduled_time_utc.astimezone(gmt7)

        # Use datetime object as key for accurate sorting
        scheduled_time_key = scheduled_time_gmt7

        # Group by scheduled_time_key (datetime object)
        if scheduled_time_key not in summary:
            summary[scheduled_time_key] = {}
        summary[scheduled_time_key][status] = order_count

    # Get the scheduled times, sort them in descending order, and take the first 7
    sorted_scheduled_times = sorted(summary.keys(), reverse=True)
    top_scheduled_times = sorted_scheduled_times[:7]

    # Display the summary for the top 7 scheduled times
    for scheduled_time_key in top_scheduled_times:
        scheduled_time_str = scheduled_time_key.strftime('%d/%m/%Y %H:%M')
        st.sidebar.write(f"**{scheduled_time_str}**")
        statuses = summary[scheduled_time_key]
        for status, count in statuses.items():
            if status == 'fulfilled':
                st.sidebar.write(f"- {count} orders fulfilled successfully")
            elif status == 'pending':
                st.sidebar.write(f"- {count} orders pending fulfillment")
            elif status == 'failed':
                st.sidebar.write(f"- {count} orders failed to fulfill")
        st.sidebar.write("---")

display_scheduled_orders()

# --- End Sidebar Code ---

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

                                # Refresh the sidebar
                                display_scheduled_orders()
                            else:
                                st.warning("No valid orders to submit.")
                except psycopg2.DatabaseError as db_error:
                    app_logger.error(f"Database error: {str(db_error)}")
                    st.error("An error occurred while saving orders to the database.")
                except Exception as e:
                    app_logger.error(f"Unexpected error: {str(e)}")
                    st.error("An unexpected error occurred.")
