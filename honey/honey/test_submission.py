import logging
import os
from honey.submitters import Radar, URLScan, Service, URLQuery, AnyRun, JoeSandbox, HybridAnalysis

if __name__ == '__main__':
    # Define format
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    formatter = logging.Formatter(log_format)

    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Stream handler (console)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Service to test
    s: Service = AnyRun()
    print(s.visibility_types)
    print(s.submission_types)

    test_url = os.getenv("TEST_URL", "https://example.com/")

    # Test submission
    print(s.submit(test_url, "pu", "w"))