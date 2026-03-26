import logging
from typing import Optional
from pipeline.db import DB
from url_token_checker.token_detector import strict_has_token_smart, is_valid_http_url


class URLTokenCheckWorker:
    """Worker to check finalurlbefore for tokens in analysis_output table."""
    
    def __init__(self, db: DB, table_name: str = "analysis_output", 
                 min_len: int = 8, min_entropy: float = 2.0):
        self.db = db
        self.table_name = table_name
        self.min_len = min_len
        self.min_entropy = min_entropy
        self.running = True
        self.column_name = "finalurlbefore_has_token"
        logging.debug(f"[URLTokenCheckWorker] Initialized for table: {table_name}")

    def run(self, batch_size: int = 1000, limit: Optional[int] = None):
        """
        Process all rows in analysis_output that need token checking.
        Uses task_phase_status for tracking and resilience.
        """
        logging.info(f"[URLTokenCheckWorker] Starting token check for table: {self.table_name}")
        
        # Initialize PENDING tasks for rows that need processing
        self.db.initialize_pending_tasks('url_token_check')
        
        processed = 0
        has_token_count = 0
        no_token_count = 0
        invalid_url_count = 0
        
        while self.running:
            # Fetch pending tasks using task tracking
            tasks = self.db.fetch_pending_tasks('url_token_check')
            
            if not tasks:
                logging.info("[URLTokenCheckWorker] No pending tasks remaining.")
                break
            
            logging.info(f"[URLTokenCheckWorker] Processing {len(tasks)} tasks...")
            
            to_update = []
            
            for source_table, source_id in tasks:
                if not self.running:
                    break
                
                # Mark as PROCESSING
                self.db.update_phase_status(source_table, source_id, 'url_token_check', 'PROCESSING')
                
                try:
                    # Get finalurlbefore
                    query = f"""
                        SELECT live_crawl_analysis -> 'before' ->> 'finalUrlBefore' as finalurlbefore
                        FROM "{self.table_name}"
                        WHERE source_table = %s AND source_id = %s
                    """
                    self.db.cursor.execute(query, (source_table, source_id))
                    result = self.db.cursor.fetchone()
                    
                    if not result or not result[0]:
                        self.db.update_phase_status(source_table, source_id, 'url_token_check', 'ERROR', None, "No finalurlbefore found")
                        continue
                    
                    finalurlbefore = result[0]
                    
                    # Validate URL and check for token
                    valid = is_valid_http_url(finalurlbefore)
                    if not valid:
                        invalid_url_count += 1
                        has_token = False
                    else:
                        has_token = strict_has_token_smart(finalurlbefore, self.min_len, self.min_entropy)
                        if has_token:
                            has_token_count += 1
                        else:
                            no_token_count += 1
                    
                    # Update analysis_output
                    to_update.append((has_token, source_table, source_id))
                    
                    # Mark as DONE
                    self.db.update_phase_status(source_table, source_id, 'url_token_check', 'DONE', has_token)
                    processed += 1
                    
                    if len(to_update) >= batch_size:
                        self._update_batch(to_update)
                        to_update.clear()
                    
                    if limit and processed >= limit:
                        break
                        
                except Exception as e:
                    error_msg = f"Error processing {source_table}:{source_id}: {e}"
                    logging.error(f"[URLTokenCheckWorker] {error_msg}")
                    self.db.update_phase_status(source_table, source_id, 'url_token_check', 'ERROR', None, error_msg)
            
            # Process remaining updates
            if to_update:
                self._update_batch(to_update)

        logging.info(f"[URLTokenCheckWorker] Completed. Processed: {processed}, "
                    f"Has token: {has_token_count}, No token: {no_token_count}, "
                    f"Invalid URLs: {invalid_url_count}")

    def _update_batch(self, to_update):
        if not to_update:
            return
        
        try:
            self.db.cursor.executemany(
                f"""
                UPDATE "{self.table_name}"
                SET "{self.column_name}" = %s
                WHERE source_table = %s AND source_id = %s
                """,
                to_update,
            )
            self.db.conn.commit()
            logging.debug(f"[URLTokenCheckWorker] Updated {len(to_update)} rows")
        except Exception as e:
            logging.error(f"[URLTokenCheckWorker] Error updating batch: {e}")
            self.db.conn.rollback()
            raise

    def run_single(self, source_table: str, source_id: int):
        """Process a single task with task tracking."""
        self.db.update_phase_status(source_table, source_id, 'url_token_check', 'PROCESSING')
        
        try:
            query = f"""
                SELECT live_crawl_analysis -> 'before' ->> 'finalUrlBefore' as finalurlbefore
                FROM "{self.table_name}"
                WHERE source_table = %s AND source_id = %s
            """
            self.db.cursor.execute(query, (source_table, source_id))
            result = self.db.cursor.fetchone()
            
            if not result or not result[0]:
                self.db.update_phase_status(source_table, source_id, 'url_token_check', 'ERROR', None, "No finalurlbefore found")
                return False
            
            finalurlbefore = result[0]
            valid = is_valid_http_url(finalurlbefore)
            
            if not valid:
                has_token = False
            else:
                has_token = strict_has_token_smart(finalurlbefore, self.min_len, self.min_entropy)
            
            self._update_batch([(has_token, source_table, source_id)])
            self.db.update_phase_status(source_table, source_id, 'url_token_check', 'DONE', has_token)
            return has_token
        except Exception as e:
            self.db.update_phase_status(source_table, source_id, 'url_token_check', 'ERROR', None, str(e))
            raise

    def shutdown(self):
        logging.debug("[URLTokenCheckWorker] Shutting down...")
        self.running = False
