# urlquery_scraper_pull.py  (Playwright version)
import time
import datetime
import hashlib
import psycopg2
from psycopg2 import extras
from bs4 import BeautifulSoup

from scraper.utils import send_discord_alert
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG

# Playwright (the same package you used in the any.run scraper)
from patchright.sync_api import sync_playwright

VERBOSE = True

def log_info(msg):
    if VERBOSE:
        print(f"[INFO] {msg}")

def log_debug(msg):
    if VERBOSE:
        print(f"[DEBUG] {msg}")

# --------------------------
# DB helpers
# --------------------------
def retry_connect_database(max_retries: int = 5, delay: int = 10):
    """Try to connect to PostgreSQL with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**LIVE_DB_CONFIG)
            log_info("Connected to DB.")
            return conn
        except Exception as e:
            log_debug(f"DB connection failed ({attempt}/{max_retries}): {e}")
            time.sleep(delay)
    send_discord_alert("URLQuery: DB connection failed after 5 attempts")
    return None

def insert_urlquery_result(db_conn, data_rows):
    """
    Insert multiple rows into urlquery_results:
    (method, time, page_url, result_url, source, page_url_hash)
    """
    if not data_rows:
        log_info("No rows to insert.")
        return

    try:
        with db_conn.cursor() as cur:
            insert_query = """
            INSERT INTO urlquery_results (
                method, time, page_url, result_url, source, page_url_hash
            )
            VALUES %s
            ON CONFLICT (page_url_hash) DO NOTHING;
            """
            extras.execute_values(cur, insert_query, data_rows, template=None)
            db_conn.commit()
            log_info(f"Inserted {len(data_rows)} new rows.")
    except Exception as e:
        log_debug(f"Insert failed: {e}")
        db_conn.rollback()
        send_discord_alert(f"URLQuery: Insert failed: {e}")

# --------------------------
# Scrape helpers (Playwright)
# --------------------------
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def fetch_recent_reports_html_pw() -> str:
    """
    Use Playwright to:
      1) visit homepage (cookie/session warm-up)
      2) request the HTMX fragment with the recent reports list
      3) return HTML for parsing
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            user_agent=UA,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        # 1) Warm-up: homepage (sets cookies / any basic session state)
        log_info("Opening homepage for cookies/session...")
        page.goto("https://urlquery.net/", wait_until="domcontentloaded", timeout=60_000)

        # 2) Fetch the HTMX list fragment
        frag_url = "https://urlquery.net/api/htmx/recent/report/11111?view=list"
        log_info("Fetching recent reports HTMX fragment...")
        page.goto(
            frag_url,
            wait_until="domcontentloaded",
            timeout=60_000,
        )

        # Optional small wait for any async fragment rendering (usually not needed)
        page.wait_for_timeout(500)

        html = page.content()

        context.close()
        browser.close()
        return html

def parse_reports(html_text: str):
    """
    Parse the HTML table rows and return a list of tuples:
      (method, time, page_url, result_url, source, page_url_hash)
    """
    soup = BeautifulSoup(html_text, "html.parser")
    rows = soup.select("tbody tr")
    log_info(f"Found {len(rows)} rows.")

    parsed_data = []
    for row in rows:
        try:
            cols = row.find_all("td")
            # Example timestamp format: "2025-08-31 19:45" (UTC)
            ts_str = cols[0].text.strip()
            timestamp = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M").replace(
                tzinfo=datetime.timezone.utc
            )

            # URL & report link are in the 3rd column
            a_tag = cols[2].find("a")
            if not a_tag or not a_tag.get("href"):
                raise ValueError("Missing anchor/href in list row.")

            page_url = a_tag.get("title") or a_tag.text.strip()
            report_path = a_tag["href"]
            report_url = "https://urlquery.net" + report_path

            page_url_hash = hashlib.md5((page_url or "").encode("utf-8")).hexdigest()

            parsed_data.append((
                "manual", timestamp, page_url, report_url, "urlquery", page_url_hash
            ))
        except Exception as e:
            log_debug(f"Row parse error: {e}")
            continue

    return parsed_data

# --------------------------
# Main
# --------------------------
def main():
    db_conn = retry_connect_database()
    if not db_conn:
        return

    try:
        html = fetch_recent_reports_html_pw()
        data = parse_reports(html)
        if data:
            insert_urlquery_result(db_conn, data)
        else:
            log_info("No data found to insert.")
    except Exception as e:
        log_debug(f"Error running main scraper: {e}")
        send_discord_alert(f"URLQuery: Scraper error: {e}")
    finally:
        try:
            db_conn.close()
        except Exception:
            pass
        log_info("Closed DB connection.")

if __name__ == "__main__":
    main()
