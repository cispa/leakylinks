import requests
import psycopg2
from psycopg2 import extras
import logging
import sys
from datetime import datetime
from scraper.utils import send_discord_alert
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG, CLOUDFLARE_API

# Setup logging to standard error
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_timestamp(timestamp_str):
    """Parses the timestamp, handling optional fractional seconds."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Timestamp '{timestamp_str}' does not match expected formats")

def fetch_and_store_cloudflare_data(api_url, headers, db_conn):
    """ Fetches live data from the Cloudflare v2 API and stores task.url in the cloudflare_results table, logs errors, and sends alerts if necessary. """
    try:
        logging.debug("Starting data fetch from Cloudflare v2 API.")
        response = requests.get(api_url, headers=headers)
        logging.debug(f"HTTP response status: {response.status_code}")

        if response.ok:
            results = response.json().get('results', [])
            logging.debug(f"Fetched {len(results)} results from Cloudflare v2 API.")

            # Use only task.url if present
            url_entries = []
            for entry in results:
                task = entry.get('task', {})
                if 'url' in task:
                    url_entries.append({
                        'url': task['url'],
                        'time': task.get('time'),
                        'uuid': task.get('uuid'),
                        'country': entry.get('page', {}).get('country'),
                        'isMalicious': entry.get('verdicts', {}).get('malicious', False),
                    })
            logging.debug(f"Trying to insert {len(url_entries)} task URLs (from v2 API).")

            with db_conn.cursor() as cur:
                # Define the SQL insert query with conflict handling and a RETURNING clause
                insert_query = """
                INSERT INTO cloudflare_results (method, time, page_url, result_url, source, country, is_malicious)
                VALUES %s ON CONFLICT (page_url_hash) DO NOTHING RETURNING id;
                """
                # Prepare the data tuples for insertion
                data_tuples = [
                    (
                        'manual',
                        parse_timestamp(entry['time']) if entry['time'] else None,
                        entry['url'],
                        f"https://radar.cloudflare.com/api/url-scanner/{entry['uuid']}" if entry['uuid'] else None,
                        'cloudflare',
                        entry['country'],
                        entry['isMalicious']
                    )
                    for entry in url_entries
                ]
                # Insert data using execute_values for batch processing and get inserted IDs
                inserted_ids = extras.execute_values(cur, insert_query, data_tuples, fetch=True)
                db_conn.commit()

                # Log the number of records actually added
                added_records = len(inserted_ids)
                logging.info(f"{added_records} new records added to cloudflare_results.")

                if added_records < len(data_tuples):
                    skipped = len(data_tuples) - added_records
                    skipped_urls = [entry['url'] for entry in url_entries][:5]
                    logging.debug(f"Skipped {skipped} existing records. Sample skipped URLs: {skipped_urls}")

                # Query to count total records in cloudflare_results
                cur.execute("SELECT COUNT(*) FROM cloudflare_results;")
                total_records = cur.fetchone()[0]
                logging.info(f"Total records in cloudflare_results table: {total_records}")

        else:
            error_msg = f"Cloudflare: Failed to fetch data, status code: {response.status_code}"
            logging.error(error_msg)
            send_discord_alert(error_msg)

    except requests.RequestException as e:
        error_msg = f"Cloudflare: HTTP request error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)

    except psycopg2.DatabaseError as e:
        error_msg = f"Cloudflare: Database error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)

    except Exception as e:
        error_msg = f"Cloudflare: Unexpected error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)
        logging.debug("Error details", exc_info=True)

def main():
    api_url = f'https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_API["account_id"]}/urlscanner/v2/search?q=verdicts.malicious:false'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {CLOUDFLARE_API["api_key"]}'
    }

    # Database connection settings
    db_conn = psycopg2.connect(**LIVE_DB_CONFIG)
    
    try:
        fetch_and_store_cloudflare_data(api_url, headers, db_conn)
    finally:
        db_conn.close()
        logging.debug("Cloudflare: Database connection closed.")

if __name__ == "__main__":
    main()
