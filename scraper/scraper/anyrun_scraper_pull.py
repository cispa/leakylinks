import json
import time
import datetime
import sys
import psycopg2
from psycopg2 import extras
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, NoSuchWindowException
from webdriver_manager.chrome import ChromeDriverManager
from scraper.utils import send_discord_alert
import shutil
import tempfile
import os
import logging
from logging.handlers import RotatingFileHandler
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG, PROJECT_PATH
import undetected_chromedriver as uc
os.environ['DISPLAY'] = ':100'
from patchright.sync_api import sync_playwright
import re


# Global variables for verbosity
VERBOSE = True

# Constants for retrying the database connection
MAX_DB_RETRIES = 5
DB_RETRY_INTERVAL = 10  # seconds
MAX_WEBHOOK_ALERTS = 3
WEBHOOK_RESET_INTERVAL = 600  # seconds (10 minutes)

# Variables for managing webhook alert limits
webhook_error_count = 0
webhook_last_reset_time = time.time()
last_discord_alert = None

# Add at the top, after other globals
consecutive_critical_failures = 0
CRITICAL_FAILURE_THRESHOLD = 5  # Stop after 5 consecutive critical failures

# Setup logging
def setup_logging():
    """Setup logging configuration with both file and console handlers."""
    # Create logs directory if it doesn't exist
    log_dir = "../logs"

    # Create a logger
    logger = logging.getLogger('anyrun_scraper')
    logger.setLevel(logging.DEBUG)

    # Create a rotating file handler (max 5 files of 5MB each)
    log_file = os.path.join(log_dir, 'anyrun_scraper.log')
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=5*1024*1024,  # 5MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatters and add them to the handlers
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Initialize logger
logger = setup_logging()

def log_info(message):
    """Log informational messages if verbosity is enabled."""
    if VERBOSE:
        logger.info(message)

def log_debug(message):
    """Log debug messages if verbosity is enabled."""
    if VERBOSE:
        logger.debug(message)

def log_error(message):
    """Log error messages."""
    logger.error(message)

def send_limited_discord_alert(message):
    """
    Sends a Discord alert with rate-limiting to prevent flooding.
    """
    global webhook_error_count, webhook_last_reset_time, last_discord_alert

    # Only send if the message is different from the last one
    if message == last_discord_alert:
        log_info("Duplicate Discord alert suppressed.")
        return
    last_discord_alert = message

    # Reset webhook alert count after the reset interval
    if time.time() - webhook_last_reset_time > WEBHOOK_RESET_INTERVAL:
        webhook_error_count = 0
        webhook_last_reset_time = time.time()

    # Send alert if below the maximum limit
    if webhook_error_count < MAX_WEBHOOK_ALERTS:
        send_discord_alert(message)
        webhook_error_count += 1
        log_info(f"Discord alert sent: {message}")
    else:
        log_info("Webhook alert suppressed to prevent flooding.")


def convert_timestamp_to_datetime(timestamp):
    """Convert a timestamp in milliseconds to a timezone-aware datetime object."""
    try:
        return datetime.datetime.fromtimestamp(timestamp / 1000, tz=datetime.timezone.utc)
    except Exception as e:
        log_debug(f"Error converting timestamp {timestamp}: {e}")
        send_discord_alert(f"Anyrun: Timestamp Conversion Error: {e}")
        return None


def retry_connect_database():
    """
    Attempt to connect to the database with retries and rate-limited webhook alerts.
    """
    attempts = 0
    while attempts < MAX_DB_RETRIES:
        try:
            db_conn = psycopg2.connect(**LIVE_DB_CONFIG)
            log_info("Successfully connected to the database.")
            return db_conn
        except psycopg2.Error as e:
            attempts += 1
            log_debug(f"Attempt {attempts} failed: {e}")
            if attempts < MAX_DB_RETRIES:
                log_info(f"Retrying database connection in {DB_RETRY_INTERVAL} seconds...")
                time.sleep(DB_RETRY_INTERVAL)
            else:
                send_limited_discord_alert(f"Anyrun: Database connection failed after {MAX_DB_RETRIES} attempts.")
    return None


def insert_into_db(db_conn, object_data):
    """
    Insert extracted data into the PostgreSQL database.
    """
    try:
        with db_conn.cursor() as cur:
            insert_query = """
            INSERT INTO anyrun_results (method, time, page_url, result_url, json_body, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (page_url_hash) DO NOTHING;
            """
            
            # Extract fields for the insertion
            method = "unknown"
            timestamp = object_data.get("fields", {}).get("times", {}).get("tryExec", {}).get("$date", None)
            if timestamp:
                time = convert_timestamp_to_datetime(timestamp)
            else:
                time = None

            page_url = object_data.get("fields", {}).get("public", {}).get("objects", {}).get("mainObject", {}).get("names", {}).get("url", None)
            uuid = object_data.get("fields", {}).get("uuid", None)
            result_url = f"https://app.any.run/tasks/{uuid}" if uuid else None
            json_body = json.dumps(object_data)
            source = "anyrun"

            # Prepare data tuple
            data_tuple = (method, time, page_url, result_url, json_body, source)

            # Execute the insert query
            cur.execute(insert_query, data_tuple)
            db_conn.commit()
            print(data_tuple)
            print("[DEBUG] Data inserted successfully.")

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        print(f"[DEBUG] {error_message}")
        send_discord_alert(f"Anyrun: Database Error: {error_message}")
        db_conn.rollback()

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        print(f"[DEBUG] {error_message}")
        send_discord_alert(f"Anyrun: Unexpected Database Error: {error_message}")


def handle_critical_failure(error_message):
    global consecutive_critical_failures
    consecutive_critical_failures += 1
    log_error(f"Critical failure #{consecutive_critical_failures}: {error_message}")

    if consecutive_critical_failures >= CRITICAL_FAILURE_THRESHOLD:
        send_limited_discord_alert(
            f"Anyrun: Stopping scraper after {CRITICAL_FAILURE_THRESHOLD} consecutive critical failures. Last error: {error_message}"
        )
        log_error("Too many consecutive failures, exiting.")
        sys.exit(1)  # Stop the script


def extract_urls(websocket_message, db_conn):
    """
    Extract data blocks from WebSocket messages if the message contains 'runType: url' related data,
    and store them in the database.
    """
    global consecutive_critical_failures
    objects = []  # To store matched data blocks
    try:
        # Ensure the message is wrapped as `a["..."]` and unwrap it
        if websocket_message.startswith('a[') and websocket_message.endswith(']'):
            json_str = websocket_message[2:-1]  # Remove `a[` and `]`
            log_debug(f"Raw JSON string: {json_str}")

            # Try parsing the JSON string
            try:
                data = json.loads(json_str)

                # Handle nested JSON (if `data` is still a string)
                if isinstance(data, str):
                    log_debug(f"Nested JSON detected. Decoding again: {data}")
                    data = json.loads(data)  # Decode again

                log_debug(f"Type after decoding: {type(data)}")
                log_debug(f"Parsed data: {data}")

                # Ensure `data` is a dictionary
                if isinstance(data, dict):
                    if data.get("msg") == "added" and data.get("collection") == "tasks":
                        fields = data.get("fields", {})
                        public_objects = fields.get("public", {}).get("objects", {})
                        run_type = public_objects.get("runType")

                        # Only process messages with runType == "url"
                        if run_type == "url":
                            objects.append(data)  # Append the entire matched data block
                            insert_into_db(db_conn, data)
                            consecutive_critical_failures = 0  # Reset on success

                else:
                    log_debug(f"Parsed data is not a dict: {type(data)} - {data}")

            except json.JSONDecodeError as e:
                log_debug(f"JSONDecodeError: {e}")
                handle_critical_failure(f"Anyrun: JSON Decode Error: {e}")
            except Exception as ex:
                if "connection already closed" in str(ex).lower():
                    log_info("WebSocket connection closed, will attempt to restart.")
                    consecutive_critical_failures = 0  # Reset on recoverable error
                else:
                    log_debug(f"Unexpected error: {ex}")
                    handle_critical_failure(f"Anyrun: Unexpected Error in JSON Parsing: {ex}")

    except Exception as e:
        log_debug(f"General error in extract_urls: {e}")
        handle_critical_failure(f"Anyrun: General Error in URL Extraction: {e}")

    return objects


def is_browser_running(driver):
    """Check if the browser is still running and responsive."""
    try:
        # Try to get the current URL - this will fail if browser is closed
        driver.current_url
        return True
    except (WebDriverException, NoSuchWindowException):
        return False


def monitor_websocket_messages_from_browser(db_conn):
    """
    Monitor WebSocket messages from AnyRun using Playwright and process them.
    """
    import re
    target_url = "https://app.any.run/submissions"
    max_retries = 3
    retry_delay = 30  # seconds
    retry_count = 0

    while retry_count < max_retries:
        try:
            with sync_playwright() as p:
                profile_dir = os.getenv("ANYRUN_PROFILE_DIR", os.path.join(tempfile.gettempdir(), "anyrun_profile"))
                context = p.chromium.launch_persistent_context(
                    user_data_dir=profile_dir,
                    #channel="chrome",
                    headless=False,
                    no_viewport=True,
                )
                page = context.new_page()
                page.goto(target_url)
                time.sleep(15)

                log_info("Monitoring WebSocket messages using Playwright WebSocket events...")
                ws_pattern = re.compile(r'^a\[.*')  # Matches the expected WebSocket message prefix

                def handle_frame(frame):
                    payload = frame
                    if isinstance(payload, bytes):
                        try:
                            payload = payload.decode('utf-8')
                        except Exception:
                            return
                    if ws_pattern.match(payload):
                        extract_urls(payload, db_conn)

                # Attach WebSocket event listeners
                def on_websocket(websocket):
                    websocket.on("framereceived", handle_frame)

                page.on("websocket", on_websocket)

                # Keep the browser open and polling
                while True:
                    try:
                        _ = page.title()
                    except Exception as e:
                        log_error(f"Browser closed or crashed: {e}. Restarting browser...")
                        break
                    time.sleep(2)
                context.close()
        except Exception as e:
            log_error(f"Error launching browser or monitoring WebSocket: {e}")
            send_discord_alert(f"Anyrun: Browser Monitoring Error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                log_info(f"Attempting to restart browser in {retry_delay} seconds... (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                log_info("Maximum retry attempts reached. Exiting.")
                send_discord_alert("Anyrun: Maximum browser restart attempts reached. Exiting.")
                break


def main():
    """Main function to monitor WebSocket messages and store results in the database."""
    global VERBOSE

    import argparse
    parser = argparse.ArgumentParser(description="Monitor WebSocket messages and filter URLs.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    VERBOSE = args.verbose

    db_conn = retry_connect_database()
    if db_conn is not None:
        try:
            monitor_websocket_messages_from_browser(db_conn)
        finally:
            db_conn.close()
    else:
        log_info("Exiting due to database connection failure.")


if __name__ == "__main__":
    main()
