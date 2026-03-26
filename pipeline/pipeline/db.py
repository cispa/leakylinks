import psycopg2
import time
import logging
from config.settings import LIVE_REM_DB_CONFIG
from config.settings import SOURCE_TABLES, LIVE_TIMING_CONFIG
from psycopg2 import pool

DB_POOL = None

def init_db_pool(db_config, minconn=5, maxconn=100):
    global DB_POOL
    if DB_POOL is None:
        DB_POOL = pool.ThreadedConnectionPool(minconn, maxconn, **db_config)

class DB:
    def __init__(self, test_mode=True):
        self.test_mode = test_mode
        self.closed = False
        db_config = LIVE_REM_DB_CONFIG
        global DB_POOL
        if DB_POOL is None:
            init_db_pool(db_config)
        self.conn = DB_POOL.getconn()
        self.conn.set_session(autocommit=True)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"SET statement_timeout = '{LIVE_TIMING_CONFIG['db_statement_timeout']}s';")
        self.cursor.execute(f"SET idle_in_transaction_session_timeout = '{LIVE_TIMING_CONFIG['db_idle_timeout']}s';")
        self.cursor.execute("SET timezone = 'UTC';")

    def fetch_pending_tasks(self, phase):
        try:
            batch_size = LIVE_TIMING_CONFIG['session_batch_size']
            valid_sources = tuple(SOURCE_TABLES.keys())
            def build_exists_check(alias):
                exists_clauses = []
                for table in valid_sources:
                    exists_clauses.append(f"""
                        SELECT 1 FROM {table} ur
                        WHERE ur.{SOURCE_TABLES[table]['id_column']} = {alias}.source_id
                        AND {alias}.source_table = '{table}'
                    """)
                return " UNION ALL ".join(exists_clauses)
            if phase == 'live_crawl':
                # For live_crawl phase, find PENDING tasks in task_phase_status (consistent with other phases)
                exists_check = build_exists_check('tps_pending')
                query = f"""
                SELECT source_table, source_id
                    FROM task_phase_status tps_pending
                    WHERE phase = %s 
                    AND status = 'PENDING'
                    AND source_table IN %s
                    AND NOT EXISTS (
                        SELECT 1 FROM task_phase_status tps_processing
                        WHERE tps_processing.source_table = tps_pending.source_table
                        AND tps_processing.source_id = tps_pending.source_id
                        AND tps_processing.phase = tps_pending.phase
                        AND tps_processing.status = 'PROCESSING'
                        AND tps_processing.updated_at > NOW() - INTERVAL '10 minutes'
                    )
                    AND EXISTS (
                        {exists_check}
                    )
                    ORDER BY tps_pending.updated_at DESC
                    LIMIT {batch_size};
                """
                self.cursor.execute(query, (phase, valid_sources))
            else:
                exists_check = build_exists_check('tps_pending')
                query = f"""
                SELECT source_table, source_id
                    FROM task_phase_status tps_pending
                    WHERE phase = %s 
                    AND status = 'PENDING'
                    AND source_table IN %s
                    AND NOT EXISTS (
                        SELECT 1 FROM task_phase_status tps_processing
                        WHERE tps_processing.source_table = tps_pending.source_table
                        AND tps_processing.source_id = tps_pending.source_id
                        AND tps_processing.phase = tps_pending.phase
                        AND tps_processing.status = 'PROCESSING'
                        AND tps_processing.updated_at > NOW() - INTERVAL '10 minutes'
                    )
                    AND EXISTS (
                        {exists_check}
                    )
                    ORDER BY tps_pending.updated_at DESC
                    LIMIT {batch_size};
                """
                self.cursor.execute(query, (phase, valid_sources))
            results = self.cursor.fetchall()
            if not results:
                return []
            return results
        except Exception as e:
            logging.error("Error fetching pending tasks: %s", e)
            self._reconnect()
            return []

    def get_url_data(self, source_table, source_id):
        try:
            config = SOURCE_TABLES[source_table]
            query = f"""
                SELECT {config['url_column']}, {config['result_column']}
                FROM {source_table}
                WHERE {config['id_column']} = %s
            """
            self.cursor.execute(query, (source_id,))
            result = self.cursor.fetchone()
            return result
        except Exception as e:
            logging.error("Error getting URL data: %s", e)
            self._reconnect()
            return None

    def update_phase_status(self, source_table, source_id, phase, status, result=None, error_message=None):
        try:
            self.cursor.execute("""
                INSERT INTO task_phase_status 
                    (source_table, source_id, phase, status, result, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_table, source_id, phase) 
                DO UPDATE SET 
                    status = EXCLUDED.status,
                    result = EXCLUDED.result,
                    error_message = EXCLUDED.error_message,
                    updated_at = now();
            """, (source_table, source_id, phase, status, result, error_message))
            self.conn.commit()
        except Exception as e:
            logging.error("Error updating phase status: %s", e)
            self.conn.rollback()
            self._reconnect()

    def initialize_pending_tasks(self, phase, query_condition=None):
        """
        Initialize PENDING tasks in task_phase_status for a phase.
        
        Args:
            phase: Phase name (e.g., 'url_token_check', 'page_difference_check')
            query_condition: Optional SQL WHERE clause to filter which rows to initialize
                            If None, uses phase-specific default logic
        """
        try:
            if phase == 'url_token_check':
                # Initialize PENDING tasks for rows that need token checking
                query = """
                    INSERT INTO task_phase_status (source_table, source_id, phase, status)
                    SELECT source_table, source_id, 'url_token_check', 'PENDING'
                    FROM analysis_output
                    WHERE live_crawl_analysis -> 'before' ->> 'finalUrlBefore' IS NOT NULL
                      AND finalurlbefore_has_token IS NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM task_phase_status tps
                          WHERE tps.source_table = analysis_output.source_table
                            AND tps.source_id = analysis_output.source_id
                            AND tps.phase = 'url_token_check'
                      )
                    ON CONFLICT (source_table, source_id, phase) DO NOTHING
                """
                self.cursor.execute(query)
                self.conn.commit()
                count = self.cursor.rowcount
                logging.info(f"Initialized {count} PENDING tasks for phase: {phase}")
                
            elif phase == 'page_difference_check':
                # Initialize PENDING tasks for rows that need page difference checking
                query = """
                    INSERT INTO task_phase_status (source_table, source_id, phase, status)
                    SELECT source_table, source_id, 'page_difference_check', 'PENDING'
                    FROM analysis_output
                    WHERE finalurlbefore_has_token = False
                      AND page_different IS NULL
                      AND live_crawl_analysis IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM task_phase_status tps
                          WHERE tps.source_table = analysis_output.source_table
                            AND tps.source_id = analysis_output.source_id
                            AND tps.phase = 'page_difference_check'
                      )
                    ON CONFLICT (source_table, source_id, phase) DO NOTHING
                """
                self.cursor.execute(query)
                self.conn.commit()
                count = self.cursor.rowcount
                logging.info(f"Initialized {count} PENDING tasks for phase: {phase}")
                
            elif phase == 'spi_detector':
                # Initialize PENDING tasks for rows that need screenshot analysis
                query = """
                    INSERT INTO task_phase_status (source_table, source_id, phase, status)
                    SELECT source_table, source_id, 'spi_detector', 'PENDING'
                    FROM analysis_output
                    WHERE not_base_domain = true 
                      AND (is_malicious = false OR is_malicious IS NULL)
                      AND (finalurlbefore_has_token = true OR page_different = true)
                      AND live_crawl_analysis IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM screenshot_analysis_results sar
                          WHERE sar.source_table = analysis_output.source_table
                            AND sar.source_id = analysis_output.source_id
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM task_phase_status tps
                          WHERE tps.source_table = analysis_output.source_table
                            AND tps.source_id = analysis_output.source_id
                            AND tps.phase = 'spi_detector'
                      )
                    ON CONFLICT (source_table, source_id, phase) DO NOTHING
                """
                self.cursor.execute(query)
                self.conn.commit()
                count = self.cursor.rowcount
                logging.info(f"Initialized {count} PENDING tasks for phase: {phase}")
            else:
                logging.warning(f"Unknown phase for initialization: {phase}")
        except Exception as e:
            logging.error(f"Error initializing pending tasks for {phase}: {e}")
            self.conn.rollback()
            self._reconnect()

    def _reconnect(self):
        try:
            if hasattr(self, 'cursor') and self.cursor:
                try:
                    self.cursor.close()
                except:
                    pass
            if hasattr(self, 'conn') and self.conn and not self.conn.closed:
                try:
                    self.conn.close()
                except:
                    pass
            db_config = LIVE_REM_DB_CONFIG
            self.conn = DB_POOL.getconn()
            self.conn.set_session(autocommit=True)
            self.cursor = self.conn.cursor()
            self.cursor.execute(f"SET statement_timeout = '{LIVE_TIMING_CONFIG['db_statement_timeout']}s';")
            self.cursor.execute(f"SET idle_in_transaction_session_timeout = '{LIVE_TIMING_CONFIG['db_idle_timeout']}s';")
            self.cursor.execute("SET timezone = 'UTC';")
            self.closed = False
            logging.info("Successfully reconnected to database")
        except Exception as e:
            logging.error("Error reconnecting to database: %s", e)
            time.sleep(LIVE_TIMING_CONFIG['db_reconnect_sleep'])

    def close(self):
        try:
            # Make close idempotent
            if getattr(self, 'closed', False) or not hasattr(self, 'conn') or self.conn is None:
                return
            if not self.conn.closed:
                try:
                    self.conn.commit()
                except Exception:
                    pass
                try:
                    if hasattr(self, 'cursor') and self.cursor:
                        self.cursor.close()
                except Exception:
                    pass
                global DB_POOL
                if DB_POOL is not None:
                    try:
                        DB_POOL.putconn(self.conn)
                    except Exception as e:
                        # Fallback: if the pool rejects the connection, close it directly
                        logging.debug("Pool.putconn failed, closing connection directly: %s", e)
                        try:
                            self.conn.close()
                        except Exception:
                            pass
                else:
                    self.conn.close()
            # Mark as closed and drop references
            self.closed = True
            self.cursor = None
            self.conn = None
        except Exception as e:
            logging.error("Error closing database connection: %s", e)

    def update_live_crawl_result(self, source_table, source_id, crawl_data):
        """Update analysis_output with live crawl results - saves JSON and sets is_malicious if unsafe"""
        try:
            import json
            # Store the full crawl data as JSON
            crawl_analysis_json = json.dumps(crawl_data) if crawl_data else None
            
            # Check Google Transparency result and set is_malicious if unsafe
            is_malicious = False
            google_transparency = crawl_data.get('googleTransparency', {})
            if google_transparency.get('status') == 'unsafe':
                is_malicious = True
                logging.info(f"[DB] Marking {source_table}:{source_id} as malicious (Google Transparency: unsafe)")
            
            self.cursor.execute("""
                UPDATE analysis_output 
                SET live_crawl_analysis = %s::jsonb,
                    live_crawl_updated_at = now(),
                    is_malicious = %s
                WHERE source_table = %s AND source_id = %s
            """, (crawl_analysis_json, is_malicious, source_table, source_id))
            self.conn.commit()
        except Exception as e:
            logging.error("Error updating live crawl result: %s", e)
            self.conn.rollback()
            self._reconnect()

    def update_url_info(self, source_table, source_id, page_url, result_url):
        try:
            self.cursor.execute("""
                UPDATE analysis_output 
                SET page_url = %s, result_url = %s
                WHERE source_table = %s AND source_id = %s
            """, (page_url, result_url, source_table, source_id))
            self.conn.commit()
        except Exception as e:
            logging.error("Error updating URL info: %s", e)
            self.conn.rollback()
            self._reconnect()

    def update_final_result(self, source_table, source_id, is_sensitive):
        """
        Update final sensitivity result in analysis_output.
        
        Note: This method references 'is_sensitive' column which does not exist in the schema.
        This method is deprecated and should not be used. Use screenshot_analysis_results table instead.
        """
        logging.warning("update_final_result() is deprecated - is_sensitive column does not exist in analysis_output table")
        # Method kept for backward compatibility but does nothing
        pass

    def set_is_base_domain(self, source_table, source_id, is_base_domain):
        """Set the not_base_domain flag in analysis_output (inverse of is_base_domain)"""
        try:
            # Schema has not_base_domain column, so we use inverse logic
            self.cursor.execute("""
                UPDATE analysis_output 
                SET not_base_domain = %s
                WHERE source_table = %s AND source_id = %s
            """, (not is_base_domain, source_table, source_id))
            self.conn.commit()
        except Exception as e:
            logging.error("Error setting base domain flag: %s", e)
            self.conn.rollback()
            self._reconnect() 