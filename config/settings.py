import os
from dotenv import load_dotenv
import requests
import logging
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv()
PROJECT_PATH = os.getenv('PROJECT_PATH')

# Unified default model name (change here to affect all pipelines unless overridden by specific env vars)
MODEL_NAME = os.getenv('MODEL_NAME', 'qwen3-vl:2b-instruct')


LIVE_REM_DB_CONFIG = {
    'host': os.getenv('LIVE_REM_DB_HOST'),
    'user': os.getenv('LIVE_REM_DB_USER'),
    'password': os.getenv('LIVE_REM_DB_PASSWORD'),
    'dbname': os.getenv('LIVE_REM_DB_NAME'),
    'port': int(os.getenv('LIVE_REM_DB_PORT', '5432')),  # Default 5432 for Docker, use 5433 for host
}


CLOUDFLARE_API = {
    'account_id': os.getenv('CLOUDFLARE_ACCOUNT_ID'),
    'email': os.getenv('CLOUDFLARE_EMAIL'),
    'api_key': os.getenv('CLOUDFLARE_API_KEY'),
    'base_url': 'https://api.cloudflare.com/client/v4/accounts/{account_id}/urlscanner/v2/result/{scan_id}'
}

URLSCAN_API = {
    'email': os.getenv('URLSCAN_EMAIL'),
    'password': os.getenv('URLSCAN_PASSWORD')
}

JOE_APIKEY = os.getenv('JOE_APIKEY')


MODEL_SERVER = {
    'host': 'ollama',
    'port': int(11434),
    'endpoint': '/api/generate',
    'timeout': int(5),
    'alert_interval': int(600),  # seconds
}


LIVE_MODEL_CONFIG = {
    'second_filter': {
        'name': os.getenv('SECOND_FILTER_MODEL', MODEL_NAME),
        'temperature': float(0),
        'promptno': os.getenv('SECOND_FILTER_PROMPTNO', 'dev')
    }
}

# --- Pipeline/Model Timeouts and Paths ---
BASE_SNAPSHOT_DIR = os.getenv('BASE_SNAPSHOT_DIR', f'{PROJECT_PATH}/live_snapshots') 
REFINED_RESULTS_TABLE = 'refined_output'
CLOUDFLARE_CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH', '$HOME/.wdm/drivers/chromedriver/linux64/136.0.7103.113/chromedriver')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')



#REFINER CONFIGS
###############################

"""Configuration settings for the refiner module."""

# Timing configuration
REFINER_TIMING_CONFIG = {
    'api_timeout': 20,  # seconds
    'db_statement_timeout': 30,  # seconds
    'anyrun_refinement_delay': 30,  # seconds - delay between parallel anyrun refinements
}

# Path to the anyrun.mjs script
JOE_MJS_PATH = f'{PROJECT_PATH}/refiner/refiner/joe.mjs'

# AnyRun profile directory
ANYRUN_PROFILE_DIR = f'{PROJECT_PATH}/refiner/refiner/data_dirs/anyrun_human'

# Map service to table and columns
SERVICE_TABLE_MAP = {
    "urlscan": ("urlscan_results", "id", ["result_url"]),
    "cloudflare": ("cloudflare_results", "id", ["result_url"]),
    "urlquery": ("urlquery_results", "id", ["result_url", "screenshot_url"]),
    "hybrid_analysis": ("hybrid_analysis_results", "id", ["result_url", "screenshot_url"]),
    "anyrun": ("anyrun_results", "id", ["result_url", "json_body"]),
    "joe": ("joe_results", "id", ["result_url", "json_body"]),
}

SERVICE_LIST = list(SERVICE_TABLE_MAP.keys())

###########################


#Live Pipeline Configs
###############################
"""Configuration settings for the live pipeline."""
# Source table configurations (static, not sensitive)
SOURCE_TABLES = {
    'urlscan_results': {
        'id_column': 'id',
        'url_column': 'page_url',
        'result_column': 'result_url',
        'enabled': True
    },
    'anyrun_results': {
        'id_column': 'id',
        'url_column': 'page_url',
        'result_column': 'result_url',
        'enabled': True
    },
    'cloudflare_results': {
        'id_column': 'id',
        'url_column': 'page_url',
        'result_column': 'result_url',
        'enabled': True
    },
    'joe_results': {
        'id_column': 'id',
        'url_column': 'page_url',
        'result_column': 'result_url',
        'enabled': True
    },
    'hybrid_analysis_results': {
        'id_column': 'id',
        'url_column': 'page_url',
        'result_column': 'result_url',
        'enabled': True
    },
    'urlquery_results': {
        'id_column': 'id',
        'url_column': 'page_url',
        'result_column': 'result_url',
        'enabled': True
    }
}

# Alert settings
ALERT_CONFIG = {
    'model_alert_interval': 600  # seconds
}

# Timing configuration
LIVE_TIMING_CONFIG = {
    'db_statement_timeout': 30,
    'db_idle_timeout': 60,
    'db_reconnect_sleep': 2,
    'pipeline_main_sleep': 5,  
    'thread_full_sleep': 0.5,  
    'session_detection_timeout': 180,
    'session_batch_size': 5,  # Number of tasks to fetch and process together  
    'batch_size': 1,
}

# Xvfb pool configuration for state drop
XVFB_CONFIG = {
    'base_display': 150,         # Start Xvfb servers at :150, :151, ...
    'num_displays': 5,           # Number of Xvfb servers to use in the pool
    'screen': '1920x1080x24',    # Screen size/depth for Xvfb
}

###########################


# Service mapping: database table names to refiner service names
SERVICE_MAPPING = {
    'anyrun_results': 'anyrun',
    'urlscan_results': 'urlscan',
    'cloudflare_results': 'cloudflare',
    'urlquery_results': 'urlquery',
    'hybrid_analysis_results': 'hybrid_analysis',
    'joe_results': 'joe',
}

# Maximum allowed image dimensions for OCR/image processing
MAX_IMAGE_WIDTH = 4000
MAX_IMAGE_HEIGHT = 20000
MAX_IMAGE_PIXELS = 80000000


# SPI Detector (Screenshot Analysis) Configuration
SPI_DETECTOR_CONFIG = {
    'model_name': os.getenv('SPI_DETECTOR_MODEL', MODEL_NAME),
    'temperature': float(os.getenv('SPI_DETECTOR_TEMPERATURE', '0.0')),
    'seed': int(os.getenv('SPI_DETECTOR_SEED', '42')),
    'timeout': int(os.getenv('SPI_DETECTOR_TIMEOUT', '300')),  # seconds
    'round_robin_host': os.getenv('SPI_DETECTOR_ROUND_ROBIN_HOST', 'http://127.0.0.1'),
    'round_robin_ports': [
        int(p.strip()) for p in os.getenv('SPI_DETECTOR_ROUND_ROBIN_PORTS', '52000,53000,54000,55000,62000,63000,64000,65000').split(',')
    ],
    # Filter settings
    'filter_min_ocr_chars': os.getenv('SPI_DETECTOR_FILTER_MIN_OCR_CHARS', 'false').lower() == 'true',
    'min_ocr_chars_threshold': int(os.getenv('SPI_DETECTOR_MIN_OCR_CHARS_THRESHOLD', '10')),
    'filter_non_latin_script': os.getenv('SPI_DETECTOR_FILTER_NON_LATIN_SCRIPT', 'false').lower() == 'true',
    'filter_tall_images': os.getenv('SPI_DETECTOR_FILTER_TALL_IMAGES', 'false').lower() == 'true',
    'filter_large_images': os.getenv('SPI_DETECTOR_FILTER_LARGE_IMAGES', 'false').lower() == 'true',
    # OCR settings
    'ocr_max_height': int(os.getenv('SPI_DETECTOR_OCR_MAX_HEIGHT', '10000')),
    'ocr_max_pixels': int(os.getenv('SPI_DETECTOR_OCR_MAX_PIXELS', '50000000')),
    'ocr_latin_threshold': float(os.getenv('SPI_DETECTOR_OCR_LATIN_THRESHOLD', '0.6')),
    'ocr_multi_lang_fallback': os.getenv('SPI_DETECTOR_OCR_MULTI_LANG_FALLBACK', 'eng+deu+fra+spa+ita+chi_sim+jpn+kor+ara+rus+hin+heb+tha'),
    'pil_max_image_pixels': int(os.getenv('SPI_DETECTOR_PIL_MAX_IMAGE_PIXELS', '200000000')),  # PIL decompression bomb protection
}

# OLLAMA_BASE_URL for SPI detector
OLLAMA_BASE_URL = os.getenv('OLLAMA_BASE_URL', 'http://ollama:11434')
