import requests
import psycopg2
from psycopg2 import extras
import logging
import sys
from scraper.utils import send_discord_alert
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG

# Setup logging to standard error
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_and_store_data(base_url, db_conn):
    """ Fetches live data from the URL and stores it in the database, logs errors and debugs the process. """
    try:
        logging.debug("Starting data fetch from URL.")
        response = requests.get(f"{base_url}/json/live")
        logging.info(f"HTTP response received: {response.status_code}")
        if response.ok:
            results = response.json().get('results', [])
            logging.debug(f"Fetched {len(results)} results from URL.")
            logging.info(f"Fetched {len(results)} results.")
            with db_conn.cursor() as cur:
                # Corrected query for multiple placeholders
                insert_query = """
                INSERT INTO urlscan_results (method, time, page_url, result_url, source)
                VALUES %s ON CONFLICT (page_url_hash) DO NOTHING;
                """
                data_tuples = [
                    (result['task']['method'], result['task']['time'], result['task']['url'],
                     result['result'], 'urlscan')
                    for result in results if 'task' in result and 'url' in result['task']
                ]
                # Correct usage of execute_values
                extras.execute_values(cur, insert_query, data_tuples)
                db_conn.commit()

                added_records = cur.rowcount
                logging.info(f"{added_records} new records added.")

                cur.execute("SELECT COUNT(*) FROM urlscan_results;")
                total_records = cur.fetchone()[0]
                logging.info(f"Total records in the urlscan_results table: {total_records}")
        else:
            error_msg = f"Urlscan: Failed to fetch data, status code: {response.status_code}"
            logging.error(error_msg)
            send_discord_alert(error_msg)
    except requests.RequestException as e:
        error_msg = f"Urlscan: HTTP request error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)
    except psycopg2.DatabaseError as e:
        error_msg = f"Urlscan: Database error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)
    except Exception as e:
        error_msg = f"Urlscan: Unexpected error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)
        logging.debug("Error details", exc_info=True)  # Provides a traceback for the exception


def main():
    base_url = 'https://urlscan.io'
    db_conn = psycopg2.connect(**LIVE_DB_CONFIG)
    try:
        fetch_and_store_data(base_url, db_conn)
    finally:
        db_conn.close()


if __name__ == "__main__":
    main()
