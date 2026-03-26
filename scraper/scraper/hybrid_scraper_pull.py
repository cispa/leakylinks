import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime
import logging
import sys
from scraper.utils import send_discord_alert
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG

# Setup logging to standard error
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_store_data(base_url, db_conn):
    """ Fetches live data from the URL and stores it in the hybrid_analysis_results table, logs errors, and sends alerts if necessary. """
    try:
        logging.debug("Starting data fetch from URL.")
        response = requests.get(f"{base_url}/feed?json")
        logging.debug(f"HTTP response status: {response.status_code}")
        
        if response.ok:
            results = response.json().get('data', [])
            logging.debug(f"Fetched {len(results)} results from URL.")
            
            with db_conn.cursor() as cur:
                # Define the SQL insert query with conflict handling and a RETURNING clause
                insert_query = """
                INSERT INTO hybrid_analysis_results (method, time, page_url, result_url, source)
                VALUES %s ON CONFLICT (page_url_hash) DO NOTHING RETURNING id;
                """
                
                # Prepare the data tuples for insertion
                data_tuples = [
                    (
                        'manual',
                        datetime.strptime(result['analysis_start_time'], "%Y-%m-%d %H:%M:%S") if 'analysis_start_time' in result else None,
                        result.get('submitname', 'N/A'),
                        f"https://hybrid-analysis.com{result['reporturl']}",
                        'hybrid-analysis'
                    )
                    for result in results if result.get('isurlanalysis') is True and 'submitname' in result
                ]
                
                # Insert data using execute_values for batch processing and get inserted IDs
                inserted_ids = extras.execute_values(cur, insert_query, data_tuples, fetch=True)
                db_conn.commit()

                # Log the number of records actually added
                added_records = len(inserted_ids)
                logging.info(f"{added_records} new records added to hybrid_analysis_results.")

                # Query to count total records in hybrid_analysis_results
                cur.execute("SELECT COUNT(*) FROM hybrid_analysis_results;")
                total_records = cur.fetchone()[0]
                logging.info(f"Total records in hybrid_analysis_results table: {total_records}")

        else:
            error_msg = f"Hybrid: Failed to fetch data, status code: {response.status_code}"
            logging.error(error_msg)
            send_discord_alert(error_msg)

    except requests.RequestException as e:
        error_msg = f"Hybrid: HTTP request error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)

    except psycopg2.DatabaseError as e:
        error_msg = f"Hybrid: Database error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)

    except Exception as e:
        error_msg = f"Hybrid: Unexpected error: {e}"
        logging.error(error_msg)
        send_discord_alert(error_msg)
        logging.debug("Error details", exc_info=True)  # Provides a traceback for the exception

def main():
    base_url = 'https://hybrid-analysis.com'
    # Database connection settings
    db_conn = psycopg2.connect(**LIVE_DB_CONFIG)
    
    try:
        fetch_and_store_data(base_url, db_conn)
    finally:
        db_conn.close()
        logging.debug("Hybrid: Database connection closed.")

if __name__ == "__main__":
    main()
