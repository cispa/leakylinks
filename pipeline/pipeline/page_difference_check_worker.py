import logging
from typing import Optional
from pipeline.db import DB
from page_difference_checker.helper import build_page_stats_from_html
from page_difference_checker.similarity import compute_similarity_score


class PageDifferenceCheckWorker:
    """Worker to check page similarity for URLs without tokens."""
    
    def __init__(self, db: DB, table_name: str = "analysis_output", 
                 similarity_threshold: float = 0.75):
        self.db = db
        self.table_name = table_name
        self.similarity_threshold = similarity_threshold
        self.running = True
        self.column_name = "page_different"
        self.redirection_column = "has_redirection"
        logging.debug(f"[PageDifferenceCheckWorker] Initialized for table: {table_name}")

    def _check_redirect(self, redirects_data):
        """Extract redirect status from JSON data - only checks 'before' redirect."""
        if not redirects_data:
            return False
        import json
        # Handle different types that PostgreSQL might return
        if isinstance(redirects_data, str):
            try:
                redirects_data = json.loads(redirects_data)
            except:
                return False
        # PostgreSQL JSONB might already be a dict (via psycopg2)
        if isinstance(redirects_data, dict):
            before_value = redirects_data.get('before', False)
            # Handle boolean values (True/False) or string "true"/"false"
            if isinstance(before_value, bool):
                return before_value
            if isinstance(before_value, str):
                return before_value.lower() in ('true', '1', 'yes')
            return bool(before_value)
        return False

    def run(self, batch_size: int = 1000, limit: Optional[int] = None):
        """
        Process all rows that need page difference checking.
        Uses task_phase_status for tracking and resilience.
        """
        logging.info(f"[PageDifferenceCheckWorker] Starting page difference check for table: {self.table_name}")
        
        # Initialize PENDING tasks for rows that need processing
        self.db.initialize_pending_tasks('page_difference_check')
        
        processed = 0
        similar_count = 0
        different_count = 0
        error_count = 0
        redirect_count = 0
        
        while self.running:
            # Fetch pending tasks using task tracking
            tasks = self.db.fetch_pending_tasks('page_difference_check')
            
            if not tasks:
                logging.info("[PageDifferenceCheckWorker] No pending tasks remaining.")
                break
            
            logging.info(f"[PageDifferenceCheckWorker] Processing {len(tasks)} tasks...")
            
            to_update = []
            
            for source_table, source_id in tasks:
                if not self.running:
                    logging.info("[PageDifferenceCheckWorker] Shutdown requested, stopping...")
                    break
                
                # Mark as PROCESSING
                self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'PROCESSING')
                
                try:
                    # Get data from analysis_output
                    query = f"""
                        SELECT live_crawl_analysis -> 'before' ->> 'finalUrlBefore' as finalurlbefore,
                               live_crawl_analysis -> 'redirects' as redirects_data
                        FROM "{self.table_name}"
                        WHERE source_table = %s AND source_id = %s
                    """
                    self.db.cursor.execute(query, (source_table, source_id))
                    result = self.db.cursor.fetchone()
                    
                    if not result:
                        self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'ERROR', None, "No data found")
                        error_count += 1
                        continue
                    
                    finalurlbefore, redirects_data = result
                    
                    # Debug: log what we got from database
                    logging.debug(f"[PageDifferenceCheckWorker] {source_table}:{source_id} - "
                                f"redirects_data type: {type(redirects_data)}, "
                                f"redirects_data value: {redirects_data}")
                    
                    # Load HTML from artifact files
                    from config.settings import SERVICE_MAPPING, BASE_SNAPSHOT_DIR
                    import os
                    
                    service_name = SERVICE_MAPPING.get(source_table, source_table)
                    snapshot_base = os.path.join(BASE_SNAPSHOT_DIR, service_name, str(source_id))
                    
                    if not os.path.exists(snapshot_base):
                        has_redirect = self._check_redirect(redirects_data)
                        to_update.append((has_redirect, False, source_table, source_id))
                        self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', False)
                        processed += 1
                        continue
                    
                    # Find most recent timestamp directory
                    timestamps = [d for d in os.listdir(snapshot_base) 
                                 if os.path.isdir(os.path.join(snapshot_base, d))]
                    if not timestamps:
                        has_redirect = self._check_redirect(redirects_data)
                        to_update.append((has_redirect, False, source_table, source_id))
                        self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', False)
                        processed += 1
                        continue
                    
                    latest = sorted(timestamps)[-1]
                    before_path = os.path.join(snapshot_base, latest, "before.html")
                    after_path = os.path.join(snapshot_base, latest, "after.html")
                    
                    if not os.path.exists(before_path) or not os.path.exists(after_path):
                        has_redirect = self._check_redirect(redirects_data)
                        to_update.append((has_redirect, False, source_table, source_id))
                        self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', False)
                        processed += 1
                        continue
                    
                    with open(before_path, 'rb') as f:
                        before_html = f.read()
                    with open(after_path, 'rb') as f:
                        after_html = f.read()
                    
                    # Build page stats
                    before_stats = build_page_stats_from_html(before_html, finalurlbefore)
                    after_stats = build_page_stats_from_html(after_html, finalurlbefore)
                    
                    # Compute similarity
                    similarity_score = compute_similarity_score(before_stats, after_stats)
                    is_different = similarity_score < self.similarity_threshold
                    
                    # Check for redirection
                    has_redirect = self._check_redirect(redirects_data)
                    
                    # Debug logging
                    logging.info(f"[PageDifferenceCheckWorker] {source_table}:{source_id} - "
                                f"similarity={similarity_score:.4f}, "
                                f"is_different={is_different}, "
                                f"has_redirect={has_redirect}, "
                                f"redirects_data={redirects_data}")
                    
                    if has_redirect:
                        redirect_count += 1
                    
                    # page_different = True only if BOTH redirection AND difference
                    page_difference_result = has_redirect and is_different
                    
                    logging.info(f"[PageDifferenceCheckWorker] {source_table}:{source_id} - "
                                f"has_redirection={has_redirect}, "
                                f"page_different={page_difference_result}")
                    
                    if is_different:
                        different_count += 1
                    else:
                        similar_count += 1
                    
                    to_update.append((
                        has_redirect,
                        page_difference_result,
                        source_table,
                        source_id,
                    ))
                    
                    # Mark as DONE
                    self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', page_difference_result)
                    processed += 1
                    
                    if len(to_update) >= batch_size:
                        self._update_batch(to_update)
                        to_update.clear()
                    
                    if limit and processed >= limit:
                        break
                        
                except Exception as e:
                    error_msg = f"Error processing {source_table}:{source_id}: {e}"
                    logging.error(f"[PageDifferenceCheckWorker] {error_msg}")
                    self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'ERROR', None, error_msg)
                    error_count += 1
                    # Try to update with default values
                    try:
                        if 'redirects_data' in locals():
                            has_redirect = self._check_redirect(redirects_data)
                            to_update.append((has_redirect, False, source_table, source_id))
                    except:
                        pass
            
            # Process remaining updates
            if to_update:
                self._update_batch(to_update)

        logging.info(f"[PageDifferenceCheckWorker] Completed. Processed: {processed}, "
                    f"Similar: {similar_count}, Different: {different_count}, "
                    f"Redirects: {redirect_count}, Errors: {error_count}")

    def _update_batch(self, to_update):
        if not to_update:
            return
        
        try:
            self.db.cursor.executemany(
                f"""
                UPDATE "{self.table_name}"
                SET "{self.redirection_column}" = %s,
                    "{self.column_name}" = %s
                WHERE source_table = %s AND source_id = %s
                """,
                to_update,
            )
            self.db.conn.commit()
            logging.debug(f"[PageDifferenceCheckWorker] Updated {len(to_update)} rows")
        except Exception as e:
            logging.error(f"[PageDifferenceCheckWorker] Error updating batch: {e}")
            self.db.conn.rollback()
            raise

    def run_single(self, source_table: str, source_id: int):
        """Process a single task with task tracking."""
        self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'PROCESSING')
        
        try:
            query = f"""
                SELECT finalurlbefore_has_token,
                       live_crawl_analysis -> 'before' ->> 'finalUrlBefore' as finalurlbefore,
                       live_crawl_analysis -> 'redirects' as redirects_data
                FROM "{self.table_name}"
                WHERE source_table = %s AND source_id = %s
            """
            self.db.cursor.execute(query, (source_table, source_id))
            result = self.db.cursor.fetchone()
            
            if not result:
                self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'ERROR', None, "No data found")
                return False
            
            has_token, finalurlbefore, redirects_data = result
            
            # Skip if has token
            if has_token:
                self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', None, "Skipped: has token")
                return None
            
            # Load HTML and compute similarity
            from config.settings import SERVICE_MAPPING, BASE_SNAPSHOT_DIR
            import os
            
            service_name = SERVICE_MAPPING.get(source_table, source_table)
            snapshot_base = os.path.join(BASE_SNAPSHOT_DIR, service_name, str(source_id))
            
            if not os.path.exists(snapshot_base):
                has_redirect = self._check_redirect(redirects_data)
                self._update_batch([(has_redirect, False, source_table, source_id)])
                self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', False)
                return False
            
            timestamps = [d for d in os.listdir(snapshot_base) 
                         if os.path.isdir(os.path.join(snapshot_base, d))]
            if not timestamps:
                has_redirect = self._check_redirect(redirects_data)
                self._update_batch([(has_redirect, False, source_table, source_id)])
                self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', False)
                return False
            
            latest = sorted(timestamps)[-1]
            before_path = os.path.join(snapshot_base, latest, "before.html")
            after_path = os.path.join(snapshot_base, latest, "after.html")
            
            if not os.path.exists(before_path) or not os.path.exists(after_path):
                has_redirect = self._check_redirect(redirects_data)
                self._update_batch([(has_redirect, False, source_table, source_id)])
                self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', False)
                return False
            
            with open(before_path, 'rb') as f:
                before_html = f.read()
            with open(after_path, 'rb') as f:
                after_html = f.read()
            
            before_stats = build_page_stats_from_html(before_html, finalurlbefore)
            after_stats = build_page_stats_from_html(after_html, finalurlbefore)
            similarity_score = compute_similarity_score(before_stats, after_stats)
            is_different = similarity_score < self.similarity_threshold
            
            # Check redirect
            has_redirect = self._check_redirect(redirects_data)
            
            # page_different = True only if BOTH redirection AND difference
            page_difference_result = has_redirect and is_different
            
            self._update_batch([(has_redirect, page_difference_result, source_table, source_id)])
            self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'DONE', page_difference_result)
            return page_difference_result
        except Exception as e:
            self.db.update_phase_status(source_table, source_id, 'page_difference_check', 'ERROR', None, str(e))
            raise

    def shutdown(self):
        logging.debug("[PageDifferenceCheckWorker] Shutting down...")
        self.running = False
