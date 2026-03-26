import psycopg2
from config.settings import LIVE_REM_DB_CONFIG

# List of DB configs to use. Add more configs here if needed.
DB_CONFIGS = [LIVE_REM_DB_CONFIG]

def get_db_connections():
    connections = []
    for config in DB_CONFIGS:
        try:
            conn = psycopg2.connect(**config)
            connections.append(conn)
        except Exception as e:
            print(f"[db_handler] Failed to connect to DB: {e}")
    return connections

def insert_to_all_dbs(query, params):
    conns = get_db_connections()
    for conn in conns:
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
        except Exception as e:
            print(f"[db_handler] Insert failed: {e}")
            conn.rollback()
        finally:
            conn.close() 