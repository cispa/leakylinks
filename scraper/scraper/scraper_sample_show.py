import psycopg2
import pandas as pd
import argparse
import warnings
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG

# Suppress specific warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

def connect_to_db():
    """
    Establishes a database connection and returns the connection object.
    """
    try:
        conn = psycopg2.connect(**LIVE_DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def fetch_data(table_name, total_only=False):
    """
    Fetches and prints the last 16 records from the specified table in a table format,
    and displays the total record count. If total_only is True, only displays the total count.
    """
    conn = connect_to_db()
    if conn is not None:
        try:
            # Setting display options for pandas
            pd.set_option('display.max_columns', None)  # No limit on the number of columns
            pd.set_option('display.max_colwidth', 50)  # Adjust this value based on your needs
            pd.set_option('display.width', 1000)  # Adjust width to your preference or terminal size

            if total_only:
                # Get total count for all three tables
                urlscan_count = pd.read_sql("SELECT COUNT(*) FROM urlscan_results;", conn).iloc[0, 0]
                hybrid_count = pd.read_sql("SELECT COUNT(*) FROM hybrid_analysis_results;", conn).iloc[0, 0]
                cloudflare_count = pd.read_sql("SELECT COUNT(*) FROM cloudflare_results;", conn).iloc[0, 0]
                anyrun_count = pd.read_sql("SELECT COUNT(*) FROM anyrun_results;", conn).iloc[0, 0]
                urlquery_count = pd.read_sql("SELECT COUNT(*) FROM urlquery_results;", conn).iloc[0, 0]
                joe_count = pd.read_sql("SELECT COUNT(*) FROM joe_results;", conn).iloc[0, 0]

                print(f"Total records in urlscan_results: {urlscan_count}")
                print(f"Total records in hybrid_analysis_results: {hybrid_count}")
                print(f"Total records in cloudflare_results: {cloudflare_count}")
                print(f"Total records in anyrun_results: {anyrun_count}")
                print(f"Total records in urlquery_results: {urlquery_count}")
                print(f"Total records in joe_results: {joe_count}")
            else:
                # Use pandas to fetch and display the last 16 records from the specified table
                df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 16;", conn)
                print(f"Fetched the last 16 records from {table_name}:")
                print(df.iloc[::-1])

                # Get the total count for the specified table
                count = pd.read_sql(f"SELECT COUNT(*) FROM {table_name};", conn).iloc[0, 0]
                print(f"Total records in {table_name}: {count}")

        except Exception as e:
            print(f"Error fetching data from the database: {e}")
        finally:
            conn.close()

def main():
    # Setting up argument parsing
    parser = argparse.ArgumentParser(description="Fetch records from the database.")
    parser.add_argument(
        'table',
        choices=['urlscan', 'hybrid', 'cloudflare', 'anyrun', 'urlquery', 'joe', 'total'],
        help="Specify which table to query: 'urlscan', 'hybrid', 'cloudflare', 'anyrun', 'urlquery', 'joe', or 'total' for all"
    )
    args = parser.parse_args()

    # Determine the table or operation based on the argument
    if args.table == 'urlscan':
        fetch_data("urlscan_results")
    elif args.table == 'hybrid':
        fetch_data("hybrid_analysis_results")
    elif args.table == 'cloudflare':
        fetch_data("cloudflare_results")
    elif args.table == 'anyrun':
        fetch_data("anyrun_results")
    elif args.table == 'urlquery':
        fetch_data("urlquery_results") 
    elif args.table == 'joe':
        fetch_data("joe_results")      
    elif args.table == 'total':
        fetch_data(None, total_only=True)

if __name__ == "__main__":
    main()
