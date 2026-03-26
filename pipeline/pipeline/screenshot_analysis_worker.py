import logging
import time
import json
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image

from pipeline.db import DB
from config.settings import BASE_SNAPSHOT_DIR, SERVICE_MAPPING
import spi_detector.analyze_screenshot as screenshot_module


class ScreenshotAnalysisWorker:
    """Worker to analyze screenshots for sensitive content using vision LLM."""
    
    def __init__(self, db: DB, table_name: str = "analysis_output", 
                 results_table: str = "screenshot_analysis_results",
                 max_workers: int = 4, max_retries: int = 2):
        self.db = db
        self.table_name = table_name
        self.results_table = results_table
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.running = True
        self.base_path = Path(BASE_SNAPSHOT_DIR)
        logging.debug(f"[ScreenshotAnalysisWorker] Initialized for table: {table_name}")

    def get_latest_screenshot_file(self, source_table: str, source_id: int) -> Optional[Path]:
        """Get the latest before.png screenshot for a given source_table and source_id."""
        service_name = SERVICE_MAPPING.get(source_table, source_table)
        base_path = self.base_path.resolve() / service_name / str(source_id)

        if not base_path.exists():
            return None

        timestamp_dirs = []
        try:
            for item in base_path.iterdir():
                if item.is_dir() and item.name.startswith('202'):
                    timestamp_dirs.append(item.name)
        except (OSError, PermissionError):
            return None

        if not timestamp_dirs:
            return None

        timestamp_dirs.sort(reverse=True)
        latest_timestamp = timestamp_dirs[0]
        screenshot_path = base_path / latest_timestamp / "before.png"

        if not screenshot_path.exists():
            return None

        return screenshot_path

    def analyze_single_screenshot(self, source_table: str, source_id: int, 
                                   page_url: str, result_url: str, 
                                   final_url_before: str = None) -> Dict:
        """
        Run screenshot analysis on a single URL.
        Returns result dict with analysis results.
        """
        start_time = time.time()

        try:
            screenshot_path = self.get_latest_screenshot_file(source_table, source_id)
            if not screenshot_path:
                return {
                    'source_table': source_table,
                    'source_id': source_id,
                    'screenshot_path': None,
                    'page_url': page_url,
                    'result_url': result_url,
                    'final_url_before': final_url_before,
                    'error_message': 'Screenshot file not found',
                    'processing_time': time.time() - start_time,
                    'analysis_timestamp': datetime.now()
                }

            # Check image size before processing
            img = Image.open(str(screenshot_path))
            width, height = img.size
            total_pixels = width * height
            
            MAX_PIXELS = 20_000_000
            MAX_HEIGHT = 15000
            
            if total_pixels > MAX_PIXELS or height > MAX_HEIGHT:
                return {
                    'source_table': source_table,
                    'source_id': source_id,
                    'screenshot_path': str(screenshot_path),
                    'page_url': page_url,
                    'result_url': result_url,
                    'final_url_before': final_url_before,
                    'error_message': f'Image too large: {width}x{height} ({total_pixels:,} pixels)',
                    'processing_time': time.time() - start_time,
                    'analysis_timestamp': datetime.now()
                }
            
            # Run vision analysis
            vision_response = screenshot_module.call_llm_with_image(
                str(screenshot_path), 
                screenshot_module.VISION_SYSTEM_PROMPT, 
                screenshot_module.VISION_USER_PROMPT
            )
            
            # Parse vision response
            vision_json_str = screenshot_module.extract_json_from_response(vision_response) if vision_response else None
            
            if vision_json_str:
                try:
                    vision_llm_obj = json.loads(vision_json_str)
                    vision_llm_obj = screenshot_module.normalize_llm_numbers(vision_llm_obj)
                except json.JSONDecodeError:
                    vision_llm_obj = None
            else:
                vision_llm_obj = None
            
            # Extract vision results
            if not vision_llm_obj:
                sensitive = False
                score = 0.0
                reasons = ["Vision LLM call failed or returned invalid JSON"]
                quoted_evidence = []
                primary_intent = None
                confidence = None
            else:
                sensitive = vision_llm_obj.get("sensitive", False)
                score = vision_llm_obj.get("risk_score", 0.0)
                reasons = vision_llm_obj.get("reasons", [])
                quoted_evidence = vision_llm_obj.get("quoted_evidence", [])
                primary_intent = vision_llm_obj.get("primary_intent")
                confidence = vision_llm_obj.get("confidence")
            
            processing_time = time.time() - start_time
            
            result = {
                'source_table': source_table,
                'source_id': source_id,
                'page_url': page_url,
                'result_url': result_url,
                'final_url_before': final_url_before,
                'screenshot_path': str(screenshot_path),
                'analysis_timestamp': datetime.now(),
                'processing_time': processing_time,
                'sensitive': sensitive,
                'score': score,
                'reasons': reasons,
                'quoted_evidence': quoted_evidence,
                'primary_intent': primary_intent,
                'confidence': confidence,
                'llm_raw': vision_llm_obj
            }

            logging.info(f"Completed {source_table}:{source_id} in {processing_time:.2f}s - "
                        f"Sensitive: {sensitive}, Score: {score:.2f}")
            return result

        except Exception as e:
            logging.error(f"Error processing {source_table}:{source_id}: {e}")
            return {
                'source_table': source_table,
                'source_id': source_id,
                'screenshot_path': None,
                'page_url': page_url,
                'result_url': result_url,
                'final_url_before': final_url_before,
                'error_message': str(e),
                'processing_time': time.time() - start_time,
                'analysis_timestamp': datetime.now()
            }

    def _ensure_table_exists(self):
        """Ensure screenshot_analysis_results table exists."""
        if not hasattr(self, '_table_checked'):
            self._table_checked = False
        
        if self._table_checked:
            return
        
        try:
            self.db.cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{self.results_table}'
                )
            """)
            exists = self.db.cursor.fetchone()[0]
            
            if not exists:
                logging.warning(f"Table {self.results_table} does not exist. Results will not be saved.")
            else:
                self._table_checked = True
        except Exception as e:
            logging.error(f"Error checking table existence: {e}")

    def _save_result(self, result: Dict):
        """Save a single result to the database."""
        if not result:
            return
        
        try:
            self._ensure_table_exists()
            
            query = f"""
                INSERT INTO {self.results_table} (
                    source_table, source_id, screenshot_path, page_url, finalurlbefore,
                    sensitive, score, reasons, quoted_evidence,
                    primary_intent, confidence,
                    processing_time, analysis_timestamp, error_message, llm_raw
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (source_table, source_id)
                DO UPDATE SET
                    screenshot_path = EXCLUDED.screenshot_path,
                    page_url = EXCLUDED.page_url,
                    finalurlbefore = EXCLUDED.finalurlbefore,
                    sensitive = EXCLUDED.sensitive,
                    score = EXCLUDED.score,
                    reasons = EXCLUDED.reasons,
                    quoted_evidence = EXCLUDED.quoted_evidence,
                    primary_intent = EXCLUDED.primary_intent,
                    confidence = EXCLUDED.confidence,
                    processing_time = EXCLUDED.processing_time,
                    analysis_timestamp = EXCLUDED.analysis_timestamp,
                    error_message = EXCLUDED.error_message,
                    llm_raw = EXCLUDED.llm_raw
            """
            
            self.db.cursor.execute(query, (
                result['source_table'],
                result['source_id'],
                result.get('screenshot_path'),
                result.get('page_url'),
                result.get('final_url_before'),
                result.get('sensitive', False),
                result.get('score', 0.0),
                result.get('reasons', []),
                result.get('quoted_evidence', []),
                result.get('primary_intent'),
                result.get('confidence'),
                result.get('processing_time', 0.0),
                result.get('analysis_timestamp'),
                result.get('error_message'),
                json.dumps(result.get('llm_raw', {}))
            ))
            self.db.conn.commit()
        except Exception as e:
            logging.error(f"Error saving result for {result.get('source_table')}:{result.get('source_id')}: {e}")
            self.db.conn.rollback()

    def run(self, batch_size: int = 50, limit: Optional[int] = None):
        """
        Process all rows that need screenshot analysis.
        Uses task_phase_status for tracking and resilience.
        """
        logging.info(f"[ScreenshotAnalysisWorker] Starting screenshot analysis for table: {self.table_name}")
        
        # Initialize PENDING tasks for rows that need processing
        self.db.initialize_pending_tasks('spi_detector')
        
        processed = 0
        errors = 0
        skipped = 0
        
        while self.running:
            # Fetch pending tasks using task tracking
            tasks = self.db.fetch_pending_tasks('spi_detector')
            
            if not tasks:
                logging.info("[ScreenshotAnalysisWorker] No pending tasks remaining.")
                break
            
            logging.info(f"[ScreenshotAnalysisWorker] Processing {len(tasks)} tasks...")
            
            # Process tasks concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_task = {}
                
                for source_table, source_id in tasks:
                    if not self.running:
                        break
                    
                    # Mark as PROCESSING
                    self.db.update_phase_status(source_table, source_id, 'spi_detector', 'PROCESSING')
                    
                    # Get URL data from analysis_output
                    query = """
                        SELECT page_url, result_url,
                               live_crawl_analysis -> 'before' ->> 'finalUrlBefore' as final_url_before
                        FROM analysis_output
                        WHERE source_table = %s AND source_id = %s
                    """
                    self.db.cursor.execute(query, (source_table, source_id))
                    result = self.db.cursor.fetchone()
                    
                    if not result:
                        self.db.update_phase_status(source_table, source_id, 'spi_detector', 'ERROR', None, "No URL data found")
                        errors += 1
                        continue
                    
                    page_url = result[0] or ''
                    result_url = result[1] or ''
                    final_url_before = result[2] or ''
                    
                    # Submit task for concurrent processing
                    future = executor.submit(
                        self.analyze_single_screenshot,
                        source_table, source_id, page_url, result_url, final_url_before
                    )
                    future_to_task[future] = (source_table, source_id)
                
                # Collect results as they complete
                for future in as_completed(future_to_task):
                    if not self.running:
                        break
                    
                    source_table, source_id = future_to_task[future]
                    
                    try:
                        result = future.result()
                        
                        # Save result
                        self._save_result(result)
                        
                        # Update phase status
                        if result.get('error_message'):
                            self.db.update_phase_status(source_table, source_id, 'spi_detector', 'ERROR', None, result['error_message'])
                            errors += 1
                        else:
                            self.db.update_phase_status(source_table, source_id, 'spi_detector', 'DONE', result.get('sensitive', False))
                        
                        processed += 1
                        
                        if limit and processed >= limit:
                            break
                            
                    except Exception as e:
                        error_msg = f"Error processing {source_table}:{source_id}: {e}"
                        logging.error(f"[ScreenshotAnalysisWorker] {error_msg}")
                        self.db.update_phase_status(source_table, source_id, 'spi_detector', 'ERROR', None, error_msg)
                        errors += 1

        logging.info(f"[ScreenshotAnalysisWorker] Completed. Processed: {processed}, Errors: {errors}, Skipped: {skipped}")

    def run_single(self, source_table: str, source_id: int):
        """Process a single task with task tracking."""
        self.db.update_phase_status(source_table, source_id, 'spi_detector', 'PROCESSING')
        
        try:
            query = """
                SELECT page_url, result_url,
                       live_crawl_analysis -> 'before' ->> 'finalUrlBefore' as final_url_before
                FROM analysis_output
                WHERE source_table = %s AND source_id = %s
            """
            self.db.cursor.execute(query, (source_table, source_id))
            result = self.db.cursor.fetchone()
            
            if not result:
                self.db.update_phase_status(source_table, source_id, 'spi_detector', 'ERROR', None, "No URL data found")
                return False
            
            page_url = result[0] or ''
            result_url = result[1] or ''
            final_url_before = result[2] or ''
            
            result = self.analyze_single_screenshot(source_table, source_id, page_url, result_url, final_url_before)
            self._save_result(result)
            
            if result.get('error_message'):
                self.db.update_phase_status(source_table, source_id, 'spi_detector', 'ERROR', None, result['error_message'])
                return False
            else:
                self.db.update_phase_status(source_table, source_id, 'spi_detector', 'DONE', result.get('sensitive', False))
                return result.get('sensitive', False)
        except Exception as e:
            self.db.update_phase_status(source_table, source_id, 'spi_detector', 'ERROR', None, str(e))
            raise

    def shutdown(self):
        logging.debug("[ScreenshotAnalysisWorker] Shutting down...")
        self.running = False

