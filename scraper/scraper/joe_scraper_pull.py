import requests
import time
import psycopg2
import json
from datetime import datetime, timezone
from config.settings import LIVE_REM_DB_CONFIG as LIVE_DB_CONFIG
from config.settings import JOE_APIKEY

global apikey
apikey = JOE_APIKEY
MAX_ITER = 10000000

def get_min_webid_from_db(db_conn):
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT MIN(webid::bigint) FROM joe_results")
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    except psycopg2.Error as e:
        print(f"[DEBUG] Failed to get min webid: {e}")
        return None

def get_max_webid_from_db(db_conn):
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT MAX(webid::bigint) FROM joe_results")
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    except psycopg2.Error as e:
        print(f"[DEBUG] Failed to get max webid: {e}")
        return None

def webid_exists_in_db(db_conn, webidx):
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM joe_results WHERE webid = %s LIMIT 1", (webidx,))
            return cur.fetchone() is not None
    except psycopg2.OperationalError as e:
        print(f"[DEBUG] Lost DB connection in webid_exists_in_db: {e}")
        raise


def giveinfo (webid): #return type of Response, the report about the webid (gives verdict + wait here)
    url = "https://www.joesandbox.com/api/v2/analysis/info"
    data = {
    "apikey": apikey,
    "webid": webid
}
    response = requests.post(url, data=data)
    return response

def givereport(webid, type): #currently saves to directory, TBD the retrieve function
    url = "https://www.joesandbox.com/api/v2/analysis/download"
    data = {
    "apikey": apikey,
    "webid": webid,
    "type": type  #or whatever type you want (shoots for screenshots)
}
    response = requests.post(url, data=data)
    # probably iterate on the types
    if response.status_code == 200:
        with open(f"{webid}.html", "wb") as f:
            f.write(response.content)
        print("Download complete.")
    else:
        print("Failed to download:", response.status_code)
        print(response.text)


def insert_joe_result(db_conn, webid, joe_result):
    try:
        with db_conn.cursor() as cur:
            insert_query = """
            INSERT INTO joe_results (webid, time, page_url, result_url, json_body, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (page_url) DO NOTHING;
            """

            data = joe_result.get("data", {})
            timestamp = datetime.now(timezone.utc)
            page_url = data.get("filename")
            result_url = f"https://www.joesandbox.com/analysis/{webid}/0/html"
            json_body = json.dumps(joe_result)
            source = "joe"

            cur.execute(insert_query, (webid, timestamp, page_url, result_url, json_body, source))
            db_conn.commit()
            print(f"[DEBUG] Inserted webid {webid}")
    except psycopg2.OperationalError as e:
        print(f"[DEBUG] Lost DB connection in insert_joe_result: {e}")
        raise
    except psycopg2.Error as e:
        print(f"[DEBUG] Database error: {e}")
        db_conn.rollback()
    except Exception as e:
        print(f"[DEBUG] Unexpected error: {e}")

    
def scrape_webids(pattern,pagination_next= None): #returns type response
    url = "https://www.joesandbox.com/api/v2/analysis/list"
    data = {
    "apikey": apikey,
    "pagination" : 1, #boolean flag
    "pagination_next" : pagination_next,
    "url": pattern
}
    response = requests.post(url, data=data)
    return response
    
def collect_webid_info(db_conn, max_iterations=1, start_webid=None, stop_webid=None):




    pagination_next = start_webid  # ← start from here if provided
    seen_webids = set()
    iteration = 0

    while True:
 
        
        iteration += 1

        response = scrape_webids("http", pagination_next=None)
        print(response.text)
        if response.status_code != 200:
            print("Failed to fetch webids:", response.status_code)
            break

        try:
            result = response.json()
        except Exception as e:
            print("Failed to parse JSON:", e)
            print("Raw response:", response.text)
            break

        runs = result.get("data", [])
        if not isinstance(runs, list):
            print("Unexpected data format for 'data':", runs)
            break

        print(f"Iteration {iteration} | Retrieved {len(runs)} webids")

        for entry in runs:
            webid = entry.get("webid")

            webid_int = int(webid)
            if stop_webid is not None and webid_int <= stop_webid:
                print(f"[INFO] Reached stop_webid: {webid_int} <= {stop_webid}")
                return

            if not webid or webid in seen_webids:
                continue

            seen_webids.add(webid)

            try:
                if webid_exists_in_db(db_conn, webidx=webid):
                    print(f"Skipping existing webid: {webid}")
                    continue
            except psycopg2.OperationalError:
                db_conn = retry_connect_database()
                if not db_conn:
                    return
                continue  # retry current webid

            info_response = giveinfo(webid)
            #print(info_response.status_code)

            if info_response.status_code == 200:
                try:
                    result_obj = info_response.json()
                    insert_joe_result(db_conn, webid, result_obj)
                except psycopg2.OperationalError:
                    db_conn = retry_connect_database()
                    if not db_conn:
                        return
                    continue  # retry current webid
            else:
                print(f"Failed to get info for webid {webid}: {info_response.status_code}")
                print(info_response.text)

            time.sleep(1)

        pagination = result.get("pagination", {})
        pagination_next = pagination.get("next")
        if not pagination_next or (max_iterations and iteration >= max_iterations):
            break


def retry_connect_database(max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(**LIVE_DB_CONFIG)
        except psycopg2.Error as e:
            print(f"[DEBUG] Failed to connect to database (attempt {attempt + 1}): {e}")
            time.sleep(delay)
    return None


# #Currently doing historical reach
# db_conn = retry_connect_database()
# if db_conn:
#     db_min = get_min_webid_from_db(db_conn)
#     db_max = get_max_webid_from_db(db_conn)

#     print(f"[INFO] Scraping NEWEST webids down to DB max ({db_max})")
#     collect_webid_info(db_conn, max_iterations=MAX_ITER, start_webid=None, stop_webid=db_max)

#     print(f"[INFO] Scraping OLDER webids down from DB min ({db_min})")
#     collect_webid_info(db_conn, max_iterations=MAX_ITER, start_webid=db_min, stop_webid=1)

#     db_conn.close()

if __name__ == "__main__":
    db_conn = retry_connect_database()
    if db_conn:
        db_max = get_max_webid_from_db(db_conn)

        print(f"[INFO] Live mode: scraping from latest down to DB max ({db_max})")
        collect_webid_info(
            db_conn,
            max_iterations=5,  # just a few pages each time
            start_webid=None,
            stop_webid=db_max
        )

        db_conn.close()
