import argparse
import logging
import sys
import os
from datetime import datetime
from config.settings import LIVE_REM_DB_CONFIG
from pipeline.db import init_db_pool, DB
from pipeline.crawl_worker import CrawlWorker
from pipeline.url_token_check_worker import URLTokenCheckWorker
from pipeline.page_difference_check_worker import PageDifferenceCheckWorker
from pipeline.screenshot_analysis_worker import ScreenshotAnalysisWorker

def setup_logging(log_level):
    log_dir = "logs/pipeline"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"pipeline_{timestamp}.log")

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
        filename=log_filename,
        filemode='w'
    )

    console = logging.StreamHandler()
    console.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logging.info("Logging initialized. Log file: %s", log_filename)

def main():
    parser = argparse.ArgumentParser(description="Pipeline runner for different phases")
    parser.add_argument("--crawl", action="store_true", help="Run crawl phase: fetch URLs and run live_crawl.js")
    parser.add_argument("--url_token_check", action="store_true", help="Run URL token check worker")
    parser.add_argument("--page_difference_check", action="store_true", help="Run page difference check worker")
    parser.add_argument("--spi_detector", action="store_true", help="Run SPI detector (screenshot analysis) for URLs with tokens or page differences")
    parser.add_argument("--log-level", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)")
    
    args = parser.parse_args()
    
    if not (args.crawl or args.url_token_check or args.page_difference_check or args.spi_detector):
        parser.print_help()
        sys.exit(1)
    
    setup_logging(args.log_level)
    start_time = datetime.now()
    
    # Initialize DB
    init_db_pool(LIVE_REM_DB_CONFIG, minconn=5, maxconn=1200)
    db = None
    
    try:
        if args.crawl:
            # Create crawl worker and run
            db = DB(test_mode=False)
            crawl_worker = CrawlWorker(db, live=True, prompt_text=None, ollama_client=None)
            try:
                crawl_worker.run()
            finally:
                crawl_worker.shutdown()
        elif args.url_token_check:
            # Create URL token check worker and run
            db = DB(test_mode=False)
            url_token_check_worker = URLTokenCheckWorker(db)
            try:
                url_token_check_worker.run(batch_size=1000)
            finally:
                url_token_check_worker.shutdown()
        elif args.page_difference_check:
            # Create page difference check worker and run
            db = DB(test_mode=False)
            page_difference_worker = PageDifferenceCheckWorker(db)
            try:
                page_difference_worker.run(batch_size=1000)
            finally:
                page_difference_worker.shutdown()
        elif args.spi_detector:
            # Create screenshot analysis worker and run
            db = DB(test_mode=False)
            screenshot_worker = ScreenshotAnalysisWorker(db, max_workers=4)
            try:
                screenshot_worker.run(batch_size=50)
            finally:
                screenshot_worker.shutdown()
    except Exception as e:
        logging.error("Fatal error: %s", e, exc_info=True)
    finally:
        logging.info("Shutting down...")
        if db:
            try:
                db.close()
            except Exception as e:
                logging.warning("Error during cleanup: %s", e, exc_info=True)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Shutdown complete. Duration: {duration:.2f}s")

if __name__ == "__main__":
    main()
