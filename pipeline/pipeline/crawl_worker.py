import os
import json
import logging
import subprocess
import psutil
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError
from multiprocessing import Manager
from config.settings import LIVE_TIMING_CONFIG, XVFB_CONFIG, PROJECT_PATH, SERVICE_MAPPING
from pipeline.utils import get_snapshot_paths
from pipeline.display_manager import DisplayManager
import signal
import re
from threading import Lock
from urllib.parse import urlparse

class CrawlWorker:
    _instance_created = False  # Class-level flag to prevent multiple instances

    def __init__(self, db, live=False, prompt_text=None, ollama_client=None):
        # Note: prompt_text and ollama_client parameters are kept for backward compatibility but are not used
        self.db = db
        self.running = True
        # Resolve Node.js helper scripts relative to project root
        self.crawl_script = os.path.join(PROJECT_PATH, "live_crawl", "live_crawl.js")
        self.crawl_pids = []
        self._pid_lock = Lock()  # Shared list of all Node.js PIDs started by this worker
        self.node_processes = []  # Track all Node.js subprocesses
        if not os.path.exists(self.crawl_script):
            raise FileNotFoundError(f"Live crawl script not found: {self.crawl_script}")
        self.display_manager = DisplayManager(
            base_display=XVFB_CONFIG.get('base_display', 10),
            num_displays=XVFB_CONFIG.get('num_displays', 2),
            screen=XVFB_CONFIG.get('screen', '1280x800x24')
        )
        self.live = live
        self.executor = ProcessPoolExecutor(max_workers=LIVE_TIMING_CONFIG.get('session_batch_size', 5))
        logging.debug(f"[CrawlWorker] Initialized with Node.js live crawl (DisplayManager mode). PID={os.getpid()}")

    @staticmethod
    def ensure_url_scheme(url):
        if not re.match(r'^https?://', url, re.IGNORECASE):
            return 'https://' + url
        return url

    @staticmethod
    def is_base_domain(url):
        """Check if URL is a base domain (no path, query, or fragment beyond root)"""
        try:
            # Ensure URL has a scheme for proper parsing
            url_with_scheme = CrawlWorker.ensure_url_scheme(url)
            parsed = urlparse(url_with_scheme)
            # Base domain if:
            # - path is empty or just "/"
            # - no query parameters
            # - no fragment
            path = parsed.path.strip()
            is_base = (not path or path == '/') and not parsed.query and not parsed.fragment
            return is_base
        except Exception as e:
            logging.warning(f"Error checking if URL is base domain: {e}")
            return False

    @staticmethod
    def run_live_crawl(script_path, page_url, artifact_paths_json, timeout=300, display_id=None):
        page_url = CrawlWorker.ensure_url_scheme(page_url)
        env = os.environ.copy()
        if display_id is not None:
            env["DISPLAY"] = f":{display_id}"
        process = subprocess.Popen(
            ["node", script_path, "--url", page_url, "--artifact_paths", artifact_paths_json],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True  # Makes the process the leader of a new process group
        )
        logging.debug(f"[CrawlWorker] Spawned Node.js subprocess: PID={process.pid}, Parent={os.getpid()}")

        # Register process in a global list for shutdown (if available)
        if hasattr(CrawlWorker, '_global_node_processes'):
            CrawlWorker._global_node_processes.append(process)

        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except Exception:
            # Kill the whole process group on error/timeout
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except Exception as e:
                logging.error(f"Failed to kill process group: {e}")
            try:
                process.wait(timeout=5)
            except Exception:
                pass
            raise
        finally:
            try:
                if process.stdout:
                    process.stdout.close()
                if process.stderr:
                    process.stderr.close()
            except Exception as e:
                logging.warning(f"Error closing process streams: {e}")
            try:
                process.wait()  # Safe even if already waited
            except Exception:
                pass
            logging.debug(f"[CrawlWorker] Node.js subprocess terminated: PID={process.pid}, ReturnCode={process.returncode}")

        output_lines = stdout.strip().splitlines()
        if not output_lines:
            raise RuntimeError(
                f"No output from Node.js live crawl.\n"
                f"Stderr: {stderr}\n"
                f"Return code: {process.returncode}\n"
                f"Env DISPLAY: {env.get('DISPLAY')}\n"
                f"Command: node {script_path} --url {page_url} --artifact_paths {artifact_paths_json}"
            )
        # Find the last JSON block in the output (robust to pretty-printing)
        json_start = None
        json_end = None
        for i in range(len(output_lines)-1, -1, -1):
            if output_lines[i].strip() == '}':
                json_end = i
                break
        if json_end is not None:
            for i in range(json_end, -1, -1):
                if output_lines[i].strip() == '{':
                    json_start = i
                    break
        if json_start is not None and json_end is not None and json_end > json_start:
            json_str = '\n'.join(output_lines[json_start:json_end+1])
            log_lines = output_lines[:json_start] + output_lines[json_end+1:]
        else:
            # fallback: treat last line as JSON (old behavior)
            json_str = output_lines[-1]
            log_lines = output_lines[:-1]
        try:
            data = json.loads(json_str)
        except Exception as e:
            raise RuntimeError(
                f"Failed to parse JSON output from Node.js live crawl.\n"
                f"Output lines: {output_lines}\n"
                f"Stderr: {stderr}\n"
                f"Error: {e}"
            )
        return {
            "crawl_data": data,
            "logs": log_lines,
            "returncode": process.returncode,
            "stderr": stderr,
            "pid": process.pid
        }

    def run(self):
        logging.debug(f"[CrawlWorker] Starting crawl (single batch mode)... PID={os.getpid()}")
        logging.debug("[CrawlWorker] About to fetch pending crawl tasks...")
        tasks = self.db.fetch_pending_tasks('live_crawl')
        logging.debug(f"[CrawlWorker] Fetched {len(tasks)} pending crawl tasks")
        if not tasks:
            logging.info("[CrawlWorker] No pending crawl tasks found. Exiting.")
            return
        logging.debug(f"[CrawlWorker] Found {len(tasks)} pending crawl tasks. Processing in parallel.")
        logging.debug("[CrawlWorker] About to call _run_batch...")
        self._run_batch(tasks)
        logging.debug("[CrawlWorker] _run_batch completed, returning to controller.")

    def run_continuous(self):
        """Run in continuous loop mode (for standalone operation)"""
        logging.debug(f"[CrawlWorker] Starting crawl loop... PID={os.getpid()}")
        while self.running:
            logging.debug(f"[CrawlWorker] Heartbeat at {datetime.now().isoformat()} — running={self.running}")
            logging.debug("[CrawlWorker] About to fetch pending crawl tasks...")
            tasks = self.db.fetch_pending_tasks('live_crawl')
            logging.debug(f"[CrawlWorker] Fetched {len(tasks)} pending crawl tasks")
            if not tasks:
                logging.info("[CrawlWorker] No pending crawl tasks found. Sleeping...")
                time.sleep(LIVE_TIMING_CONFIG.get('session_idle_sleep', 5))
                logging.debug("[CrawlWorker] Woke up from sleep, continuing loop...")
                continue
            logging.debug(f"[CrawlWorker] Found {len(tasks)} pending crawl tasks. Processing in parallel.")
            logging.debug("[CrawlWorker] About to call _run_batch...")
            self._run_batch(tasks)
            logging.debug("[CrawlWorker] _run_batch completed, continuing loop...")

    def _run_batch(self, tasks):
        logging.debug(f"[CrawlWorker] Starting _run_batch... PID={os.getpid()}")
        batch_size = LIVE_TIMING_CONFIG.get('session_batch_size', 5)
        logging.debug(f"[CrawlWorker] Using batch_size: {batch_size}")
        logging.debug("[CrawlWorker] About to submit tasks to ProcessPoolExecutor...")
        future_to_task = {}
        new_pids = []
        try:
            for idx, (source_table, source_id) in enumerate(tasks):
                result = self.db.get_url_data(source_table, source_id)
                if result is None:
                    continue
                page_url, result_url = result

                # Check if URL is a base domain - if so, skip crawling
                if CrawlWorker.is_base_domain(page_url):
                    logging.info(f"[CrawlWorker][{source_table}:{source_id}] Skipping base domain: {page_url}")
                    self.db.set_is_base_domain(source_table, source_id, True)
                    self.db.update_phase_status(source_table, source_id, 'live_crawl', 'DONE', True)
                    continue
                else:
                    # Set not_base_domain = True when it's NOT a base domain
                    self.db.set_is_base_domain(source_table, source_id, False)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                # Convert source_table to service_name using SERVICE_MAPPING
                service_name = SERVICE_MAPPING.get(source_table, source_table)
                artifact_paths = get_snapshot_paths(service_name, str(source_id), timestamp)
                artifact_paths_json = json.dumps(artifact_paths)
                self.db.update_phase_status(source_table, source_id, 'live_crawl', 'PROCESSING')
                with self.display_manager.acquire_display(idx) as display_id:
                    future = self.executor.submit(
                        CrawlWorker.run_live_crawl,
                        self.crawl_script,
                        page_url,
                        artifact_paths_json,
                        300,
                        display_id
                    )
                    future_to_task[future] = (source_table, source_id, page_url, result_url)
            logging.debug(f"[CrawlWorker] Batch started {len(new_pids)} Node.js PIDs: {', '.join(str(pid) for pid in new_pids)}")
            logging.debug("[CrawlWorker] About to wait for futures to complete...")
            try:
                for future in as_completed(future_to_task, timeout=600):  # max batch timeout
                    source_table, source_id, page_url, result_url = future_to_task[future]
                    log_prefix = f"[Crawl][{source_table}:{source_id}]"
                    logging.debug(f"[CrawlWorker] Processing completed future for {source_table}:{source_id}")
                    try:
                        result = future.result(timeout=300)  # timeout per task
                        pid = result.get("pid")
                        if pid:
                            with self._pid_lock:
                                self.crawl_pids.append(pid)
                                new_pids.append(pid)
                        # Check for error in Node.js JSON output
                        crawl_data = result["crawl_data"]
                        if crawl_data.get("error"):
                            error_msg = crawl_data["error"]
                            self.db.update_phase_status(
                                source_table, source_id, 'live_crawl', 'ERROR', None, error_msg
                            )
                            continue
                        
                        crawl_logs = {
                            "logs": result["logs"],
                            "crawl_data": crawl_data
                        }
                        logging.debug(f"{log_prefix} Crawl details:")
                        logging.debug(f"{log_prefix} URL: {page_url}")
                        logging.debug(f"{log_prefix} Node.js logs:")
                        for line in result["logs"]:
                            logging.debug(f"{log_prefix} [Node.js] {line}")
                        logging.debug(f"{log_prefix} JSON output:")
                        for line in json.dumps(crawl_data, indent=2).split('\n'):
                            logging.debug(f"{log_prefix} {line}")
                        logging.info(f"{log_prefix} Crawl completed successfully")
                        # Save crawl results to database
                        self.db.update_live_crawl_result(source_table, source_id, crawl_data)
                        self.db.update_phase_status(
                            source_table, source_id, 'live_crawl', 'DONE', True
                        )
                        logging.debug(f"[CrawlWorker] Successfully completed {source_table}:{source_id}")
                    except TimeoutError:
                        logging.error(f"[CrawlWorker] Timeout: {source_table}:{source_id} took too long. Marking as failed.")
                        self.db.update_phase_status(source_table, source_id, 'live_crawl', 'ERROR', None, "Timeout after 300s")
                        continue
                    except Exception as e:
                        error_msg = f"Crawl failed: {e}"
                        logging.error(f"{log_prefix} {error_msg}")
                        if hasattr(e, 'args') and e.args and 'Failed to parse JSON output' in str(e.args[0]):
                            logging.error(f"{log_prefix} Full Node.js output and stderr: {e.args[0]}")
                        self.db.update_phase_status(
                            source_table, source_id, 'live_crawl', 'ERROR', None, str(e)
                        )
            except TimeoutError:
                logging.error("[CrawlWorker] Batch timeout: not all crawl tasks finished in 600s.")
        finally:
            alive_pids = [pid for pid in new_pids if psutil.pid_exists(pid)]
            dead_pids = [pid for pid in new_pids if not psutil.pid_exists(pid)]
            logging.debug(f"[CrawlWorker] Batch ended. Alive: {len(alive_pids)} [{', '.join(str(pid) for pid in alive_pids)}] | Exited: {len(dead_pids)} [{', '.join(str(pid) for pid in dead_pids)}]")
            logging.debug("[CrawlWorker] Main loop: batch complete, checking for next batch or exiting.")
            logging.debug("[CrawlWorker] _run_batch completed successfully")

    def run_single(self, source_table, source_id):
        """Run crawl for a single URL"""
        try:
            result = self.db.get_url_data(source_table, source_id)
            if result is None:
                logging.error(f"[run_single] URL data not found for {source_table}:{source_id}")
                logging.info(f"[run_single] END {source_table}:{source_id} (not found)")
                return "not_found"
            page_url, result_url = result

            # Check if URL is a base domain - if so, skip crawling
            if CrawlWorker.is_base_domain(page_url):
                logging.info(f"[run_single][{source_table}:{source_id}] Skipping base domain: {page_url}")
                self.db.set_is_base_domain(source_table, source_id, True)
                self.db.update_phase_status(source_table, source_id, 'live_crawl', 'DONE', True)
                logging.info(f"[run_single] END {source_table}:{source_id} (base domain skipped)")
                return "base_domain_skipped"
            else:
                # Set not_base_domain = True when it's NOT a base domain
                self.db.set_is_base_domain(source_table, source_id, False)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Convert source_table to service_name using SERVICE_MAPPING
            service_name = SERVICE_MAPPING.get(source_table, source_table)
            artifact_paths = get_snapshot_paths(service_name, str(source_id), timestamp)
            artifact_paths_json = json.dumps(artifact_paths)
            self.db.update_phase_status(source_table, source_id, 'live_crawl', 'PROCESSING')
            logging.info(f"[run_single] Calling Node.js live crawl for {source_table}:{source_id}")
            with self.display_manager.acquire_display(0) as display_id:
                try:
                    result = CrawlWorker.run_live_crawl(
                        self.crawl_script,
                        page_url,
                        artifact_paths_json,
                        300,
                        display_id
                    )
                    logging.info(f"[run_single] Node.js live crawl returned for {source_table}:{source_id}")
                    # Check for error in Node.js JSON output
                    crawl_data = result["crawl_data"]
                    if crawl_data.get("error"):
                        error_msg = crawl_data["error"]
                        self.db.update_phase_status(
                            source_table, source_id, 'live_crawl', 'ERROR', None, error_msg
                        )
                        logging.error(f"[run_single] Node.js live crawl error for {source_table}:{source_id}: {error_msg}")
                        logging.info(f"[run_single] END {source_table}:{source_id} (node error)")
                        return "node_error"
                    
                    crawl_logs = {
                        "logs": result["logs"],
                        "crawl_data": crawl_data
                    }
                    logging.info(f"[CrawlWorker][{source_table}:{source_id}] Crawl completed successfully")
                    # Save crawl results to database
                    self.db.update_live_crawl_result(source_table, source_id, crawl_data)
                    self.db.update_phase_status(
                        source_table, source_id, 'live_crawl', 'DONE', True
                    )
                    logging.debug(f"[CrawlWorker] Successfully completed {source_table}:{source_id}")
                    logging.info(f"[run_single] END {source_table}:{source_id} (success)")
                    return "success"
                except TimeoutError:
                    logging.error(f"[CrawlWorker] Timeout: {source_table}:{source_id} took too long. Marking as failed.")
                    self.db.update_phase_status(source_table, source_id, 'live_crawl', 'ERROR', None, "Timeout after 300s")
                    logging.info(f"[run_single] END {source_table}:{source_id} (timeout)")
                    return "timeout"
                except Exception as e:
                    error_msg = f"Crawl failed: {e}"
                    logging.error(f"[CrawlWorker][{source_table}:{source_id}] {error_msg}")
                    self.db.update_phase_status(
                        source_table, source_id, 'live_crawl', 'ERROR', None, str(e)
                    )
                    logging.info(f"[run_single] END {source_table}:{source_id} (exception)")
                    return "exception"
        except Exception as e:
            logging.error(f"[run_single] Outer exception for {source_table}:{source_id}: {e}")
            logging.info(f"[run_single] END {source_table}:{source_id} (outer exception)")
            return "exception"


    def shutdown(self):
        logging.debug("Shutting down crawl worker...")
        self.running = False

        # Kill all Node.js process groups we started
        for pid in list(self.crawl_pids):
            try:
                if psutil.pid_exists(pid):
                    logging.debug(f"Killing Node.js process group with PID: {pid}")
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
            except Exception:
                # Suppress errors for already-exited or missing processes
                pass
        self.crawl_pids[:] = []  # Clear the list after killing

        # Clean up displays and shut down the executor
        try:
            self.display_manager.cleanup()
        except Exception as e:
            logging.warning(f"Error during DisplayManager cleanup: {e}")

        try:
            self.executor.shutdown(wait=True)
        except Exception as e:
            logging.warning(f"Error during ProcessPoolExecutor shutdown: {e}")

        logging.debug("Crawl worker shutdown complete")

