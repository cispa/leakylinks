import logging
import time
import threading
from datetime import datetime
import psutil
import pytz
from config.settings import SOURCE_TABLES, LIVE_TIMING_CONFIG
from pipeline.db import DB
from pipeline.crawl_worker import CrawlWorker
from pipeline.url_token_check_worker import URLTokenCheckWorker

class PipelineController:
    def __init__(self, test_mode=True, check_model_ready_per_request=False, prompt_text=None, url_token_check_mode=False):
        # Note: check_model_ready_per_request and prompt_text parameters are kept for backward compatibility but are not used
        self.db = DB(test_mode=test_mode)
        self.url_token_check_mode = url_token_check_mode
        
        if url_token_check_mode:
            self.url_token_check = URLTokenCheckWorker(self.db)
            self.crawl = None
        else:
            self.crawl = CrawlWorker(
                self.db,
                live=not test_mode,
                prompt_text=None,
                ollama_client=None
            )
            self.url_token_check = None
        self.running = True

    def get_enabled_sources(self):
        return [table for table, config in SOURCE_TABLES.items() if config['enabled']]

    def rerun_phase(self, source_table, url_id, phase):
        """Rerun only the specified phase for a given URL ID in the given source table."""
        if phase == 'live_crawl':
            if not self.crawl:
                raise ValueError("Crawl worker not initialized")
            self.crawl.run_single(source_table, url_id)
        elif phase == 'url_token_check':
            if not self.url_token_check:
                raise ValueError("URL token check worker not initialized")
            self.url_token_check.run_single(source_table, url_id)
        else:
            raise ValueError(f"Unknown phase: {phase}")

    def run_live(self):
        logging.info("[PipelineController] Starting live pipeline loop")
        
        if self.url_token_check_mode:
            # URL token check mode
            logging.info("[PipelineController] Running in URL token check mode")
            try:
                while self.running:
                    self.url_token_check.run(batch_size=1000)
                    # Check if there's more work
                    self.db.cursor.execute(
                        f"""
                        SELECT COUNT(*) 
                        FROM "{self.url_token_check.table_name}"
                        WHERE finalurlbefore IS NOT NULL
                          AND "{self.url_token_check.column_name}" IS NULL
                        """
                    )
                    remaining = self.db.cursor.fetchone()[0]
                    if remaining == 0:
                        logging.info("[PipelineController] No more rows to process. Exiting.")
                        break
                    time.sleep(LIVE_TIMING_CONFIG['pipeline_main_sleep'])
            finally:
                self.close()
        else:
            # Normal crawl mode
            phase_start_time = None
            current_phase = None
            try:
                while self.running:
                    logging.debug(f"[HEARTBEAT] Main loop alive at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    if hasattr(self, 'crawl') and hasattr(self.crawl, 'crawl_pids'):
                        tracked_pids = list(self.crawl.crawl_pids)
                        alive_pids = [pid for pid in tracked_pids if psutil.pid_exists(pid)]
                        logging.debug(f"[PipelineController] Node.js PIDs tracked: {len(tracked_pids)} | Alive: {len(alive_pids)} (PIDs: {alive_pids})")

                    # Stop if there is no work to do
                    pending_crawl = self.db.fetch_pending_tasks('live_crawl')
                    if not pending_crawl:
                        logging.info("[PipelineController] No pending tasks for crawl. Exiting.")
                        break

                    enabled_sources = self.get_enabled_sources()
                    for source in enabled_sources:
                        logging.debug("[PipelineController] Processing source: %s", source)
                    # --- Crawl Phase ---
                    if current_phase != 'live_crawl':
                        current_phase = 'live_crawl'
                        phase_start_time = time.time()
                    else:
                        if time.time() - phase_start_time > 3600:
                            phase_start_time = time.time()  # reset to avoid spamming
                    logging.debug("[PipelineController] Running crawl phase")
                    if self.crawl:
                        self.crawl.run()
                    time.sleep(LIVE_TIMING_CONFIG['pipeline_main_sleep'])
            finally:
                self.close()

    def run_batched(self):
        logging.info("[PipelineController] Starting batched pipeline loop")
        
        if self.url_token_check_mode:
            # URL token check mode
            logging.info("[PipelineController] Running in URL token check mode (batched)")
            try:
                while self.running:
                    self.url_token_check.run(batch_size=1000)
                    # Check if there's more work
                    self.db.cursor.execute(
                        f"""
                        SELECT COUNT(*) 
                        FROM "{self.url_token_check.table_name}"
                        WHERE finalurlbefore IS NOT NULL
                          AND "{self.url_token_check.column_name}" IS NULL
                        """
                    )
                    remaining = self.db.cursor.fetchone()[0]
                    if remaining == 0:
                        logging.info("[PipelineController] No more rows to process. Exiting.")
                        break
                    time.sleep(LIVE_TIMING_CONFIG['pipeline_main_sleep'])
            finally:
                self.close()
        else:
            # Normal crawl mode
            phase_start_time = None
            current_phase = None
            try:
                while self.running:
                    logging.debug(f"[HEARTBEAT] Main loop alive at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    if hasattr(self, 'crawl') and hasattr(self.crawl, 'crawl_pids'):
                        tracked_pids = list(self.crawl.crawl_pids)
                        alive_pids = [pid for pid in tracked_pids if psutil.pid_exists(pid)]
                        logging.debug(f"[PipelineController] Node.js PIDs tracked: {len(tracked_pids)} | Alive: {len(alive_pids)} (PIDs: {alive_pids})")

                    # Stop if there is no work to do
                    pending_crawl = self.db.fetch_pending_tasks('live_crawl')
                    if not pending_crawl:
                        logging.info("[PipelineController] No pending tasks for crawl (batch). Exiting.")
                        break

                    enabled_sources = self.get_enabled_sources()
                    for source in enabled_sources:
                        logging.debug("[PipelineController] Processing source: %s (batch)", source)
                    # --- Crawl Phase ---
                    if current_phase != 'live_crawl':
                        current_phase = 'live_crawl'
                        phase_start_time = time.time()
                    else:
                        if time.time() - phase_start_time > 3600:
                            phase_start_time = time.time()  # reset to avoid spamming
                    logging.debug("[PipelineController] Running crawl phase (batch)")
                    if self.crawl:
                        self.crawl.run()
                    time.sleep(LIVE_TIMING_CONFIG['pipeline_main_sleep'])
            finally:
                self.close()


    def close(self):
        """Close all resources"""
        logging.debug("[PipelineController] Shutting down pipeline controller...")
        self.running = False
        try:
            if hasattr(self, 'crawl') and self.crawl is not None:
                self.crawl.shutdown()
            if hasattr(self, 'url_token_check') and self.url_token_check is not None:
                self.url_token_check.shutdown()
            if hasattr(self, 'db'):
                self.db.close()
        except Exception as e:
            logging.error("[PipelineController] Error during shutdown: %s", e)
        logging.debug("[PipelineController] Shutdown complete.")
