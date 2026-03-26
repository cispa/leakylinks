#!/usr/bin/env python3
"""
Screenshot-based sensitivity analyzer for SPI detection.
This analyzer:
1. Takes a screenshot path as input
2. Extracts text via OCR for filtering (character count)
3. Sends screenshot directly to vision LLM model for sensitivity analysis
4. Returns analysis results (can be stored in screenshot_analysis_results table)
5. Uses LLM host/port from settings (OLLAMA_BASE_URL)

Note: OCR is only used for filtering (e.g., skip pages with too few characters).
The actual sensitivity verdict comes from vision LLM analysis only.
"""

# Standard library imports
import argparse
import json
import logging
import math
import os
import queue
import re
import sys
import threading
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any

# Third-party imports
try:
    import pytesseract
    from PIL import Image
except ImportError:
    print("ERROR: Please install required packages: pip install pytesseract Pillow")
    sys.exit(1)

try:
    from ollama import Client
except ImportError:
    print("ERROR: Please install ollama package: pip install ollama")
    sys.exit(1)

try:
    import requests
except ImportError:
    requests = None

# Local imports
try:
    from schemas import VisionAnalysisResponse
except ImportError:
    try:
        from .schemas import VisionAnalysisResponse
    except ImportError:
        logging.warning("Could not import schemas. Pydantic validation disabled.")
        VisionAnalysisResponse = None



# Settings import
try:
    from config.settings import OLLAMA_BASE_URL, SPI_DETECTOR_CONFIG
except ImportError:
    OLLAMA_BASE_URL = os.getenv('OLLAMA_BASE_URL', 'http://ollama:11434')
    SPI_DETECTOR_CONFIG = {
        'model_name': os.getenv('SPI_DETECTOR_MODEL', 'qwen3-vl:2b-instruct'),
        'temperature': float(os.getenv('SPI_DETECTOR_TEMPERATURE', '0.0')),
        'seed': int(os.getenv('SPI_DETECTOR_SEED', '42')),
        'timeout': int(os.getenv('SPI_DETECTOR_TIMEOUT', '300')),
        'round_robin_host': os.getenv('SPI_DETECTOR_ROUND_ROBIN_HOST', 'http://127.0.0.1'),
        'round_robin_ports': [int(p.strip()) for p in os.getenv('SPI_DETECTOR_ROUND_ROBIN_PORTS', '52000,53000,54000,55000,62000,63000,64000,65000').split(',')],
        'filter_min_ocr_chars': os.getenv('SPI_DETECTOR_FILTER_MIN_OCR_CHARS', 'false').lower() == 'true',
        'min_ocr_chars_threshold': int(os.getenv('SPI_DETECTOR_MIN_OCR_CHARS_THRESHOLD', '10')),

        'filter_tall_images': os.getenv('SPI_DETECTOR_FILTER_TALL_IMAGES', 'false').lower() == 'true',
        'filter_large_images': os.getenv('SPI_DETECTOR_FILTER_LARGE_IMAGES', 'false').lower() == 'true',
        'ocr_max_height': int(os.getenv('SPI_DETECTOR_OCR_MAX_HEIGHT', '10000')),
        'ocr_max_pixels': int(os.getenv('SPI_DETECTOR_OCR_MAX_PIXELS', '50000000')),

        'pil_max_image_pixels': int(os.getenv('SPI_DETECTOR_PIL_MAX_IMAGE_PIXELS', '200000000')),
    }


# ============================================================================
# CONFIGURATION
# ============================================================================

# Round-robin state for load balancing across multiple LLM ports
_round_robin_counter = 0
_round_robin_lock = threading.Lock()
_ROUND_ROBIN_HOST = SPI_DETECTOR_CONFIG['round_robin_host']  # From settings
_ROUND_ROBIN_PORTS = SPI_DETECTOR_CONFIG['round_robin_ports']  # From settings
_LLM_BASE_URL = OLLAMA_BASE_URL.rstrip('/')  # Base URL from settings for sequential mode
_LLM_MODE = "sequential"  # Default mode

_LLM_MODEL = SPI_DETECTOR_CONFIG['model_name']  # From settings
LLM_TEMPERATURE = SPI_DETECTOR_CONFIG['temperature']  # From settings
LLM_SEED = SPI_DETECTOR_CONFIG['seed']  # From settings
LLM_TIMEOUT = SPI_DETECTOR_CONFIG['timeout']  # From settings

# Filter flags from settings
FILTER_MIN_OCR_CHARS = SPI_DETECTOR_CONFIG['filter_min_ocr_chars']
MIN_OCR_CHARS_THRESHOLD = SPI_DETECTOR_CONFIG['min_ocr_chars_threshold']

FILTER_TALL_IMAGES = SPI_DETECTOR_CONFIG['filter_tall_images']
FILTER_LARGE_IMAGES = SPI_DETECTOR_CONFIG['filter_large_images']

# OCR settings from settings
OCR_MAX_HEIGHT = SPI_DETECTOR_CONFIG['ocr_max_height']
OCR_MAX_PIXELS = SPI_DETECTOR_CONFIG['ocr_max_pixels']


# Set PIL decompression bomb protection from settings
Image.MAX_IMAGE_PIXELS = SPI_DETECTOR_CONFIG['pil_max_image_pixels']

def _get_llm_host() -> str:
    """
    Get the LLM host for round-robin load balancing or sequential mode.
    Returns the configured host based on mode.
    """
    global _LLM_MODE
    
    if _LLM_MODE == "round_robin":
        global _round_robin_counter, _ROUND_ROBIN_HOST
        with _round_robin_lock:
            port = _ROUND_ROBIN_PORTS[_round_robin_counter % len(_ROUND_ROBIN_PORTS)]
            _round_robin_counter += 1
            return f"{_ROUND_ROBIN_HOST}:{port}"
    else:
        # Sequential mode - use host from settings
        return _LLM_BASE_URL

def set_llm_mode(mode: str):
    """
    Set the LLM mode dynamically.
    
    Args:
        mode: Either "sequential" or "round_robin"
    """
    global _LLM_MODE
    if mode not in ["sequential", "round_robin"]:
        raise ValueError("Mode must be either 'sequential' or 'round_robin'")
    _LLM_MODE = mode
    logging.info(f"Screenshot analyzer LLM mode set to: {mode}")

def set_round_robin_ports(ports: list):
    """
    Set the round-robin ports dynamically.
    
    Args:
        ports: List of port numbers to use for round-robin load balancing
    """
    global _ROUND_ROBIN_PORTS, _round_robin_counter
    if not ports or not isinstance(ports, list):
        raise ValueError("Ports must be a non-empty list")
    if not all(isinstance(p, int) and 1 <= p <= 65535 for p in ports):
        raise ValueError("All ports must be valid integers between 1 and 65535")
    
    with _round_robin_lock:
        _ROUND_ROBIN_PORTS = ports
        _round_robin_counter = 0  # Reset counter when ports change
    logging.info(f"Round-robin ports set to: {ports}")

def set_model(model_name: str):
    """
    Set the LLM model name dynamically.
    
    Args:
        model_name: Name of the model to use (e.g., "gemma3:27b-it-q8_0", "llama3.2:3b")
    """
    global _LLM_MODEL
    if not model_name or not isinstance(model_name, str):
        raise ValueError("Model name must be a non-empty string")
    _LLM_MODEL = model_name
    logging.info(f"LLM model set to: {model_name}")

def get_model() -> str:
    """
    Get the current LLM model name.
    
    Returns:
        Current model name
    """
    return _LLM_MODEL

def set_filters(min_ocr_chars: bool = None, 
                tall_images: bool = None, large_images: bool = None):
    """
    Enable or disable image/text filtering.
    
    Args:
        min_ocr_chars: Enable/disable minimum character count filter
        tall_images: Enable/disable tall image cropping
        large_images: Enable/disable large image size protection
    """
    global FILTER_MIN_OCR_CHARS, FILTER_TALL_IMAGES, FILTER_LARGE_IMAGES
    
    if min_ocr_chars is not None:
        FILTER_MIN_OCR_CHARS = min_ocr_chars
        logging.info(f"Filter MIN_OCR_CHARS: {'ENABLED' if min_ocr_chars else 'DISABLED'}")
    
    if tall_images is not None:
        FILTER_TALL_IMAGES = tall_images
        logging.info(f"Filter TALL_IMAGES: {'ENABLED' if tall_images else 'DISABLED'}")
    
    if large_images is not None:
        FILTER_LARGE_IMAGES = large_images
        logging.info(f"Filter LARGE_IMAGES: {'ENABLED' if large_images else 'DISABLED'}")


def check_llm_health(timeout: int = 10) -> dict:
    """
    Check health of LLM servers on configured ports.
    
    Args:
        timeout: Timeout in seconds for each health check
        
    Returns:
        Dictionary with:
            - all_healthy: bool indicating if all ports are healthy
            - ports_checked: list of port numbers checked
            - ports_failed: list of port numbers that failed
            - health_status: dict mapping port -> True/False
    """
    if requests is None:
        logging.warning("requests module not available, skipping health check")
        return {
            "all_healthy": False,
            "ports_checked": [],
            "ports_failed": [],
            "health_status": {}
        }
    
    health_status = {}
    ports_failed = []
    ports_checked = []
    
    # Determine which URLs to check based on mode
    global _LLM_MODE, _LLM_BASE_URL, _ROUND_ROBIN_PORTS
    
    if _LLM_MODE == "round_robin":
        # Round-robin mode: check all round-robin ports
        global _ROUND_ROBIN_HOST
        urls_to_check = [f"{_ROUND_ROBIN_HOST}:{port}" for port in _ROUND_ROBIN_PORTS]
    else:
        # Sequential mode: check the base URL from settings
        urls_to_check = [_LLM_BASE_URL]
    
    for url in urls_to_check:
        ports_checked.append(url)
        health_url = f"{url}/api/tags"
        
        try:
            response = requests.get(health_url, timeout=timeout)
            if response.status_code == 200:
                health_status[url] = True
                logging.info(f"{url}: Healthy")
            else:
                health_status[url] = False
                ports_failed.append(url)
                logging.warning(f"{url}: Unhealthy (status {response.status_code})")
        except requests.exceptions.RequestException as e:
            health_status[url] = False
            ports_failed.append(url)
            logging.warning(f"{url}: Unhealthy ({type(e).__name__})")
    
    all_healthy = len(ports_failed) == 0
    
    return {
        "all_healthy": all_healthy,
        "ports_checked": ports_checked,
        "ports_failed": ports_failed,
        "health_status": health_status
    }


# ============================================================================
# SCORE NORMALIZATION (0–1)
# ============================================================================

def _to_float(v):
    """
    Accept 85, 0.85, "85", "0.85", "85%"
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s.endswith("%"):
        try:
            return float(s[:-1].strip()) / 100.0
        except:
            return None
    try:
        return float(s)
    except:
        return None

def _normalize_unit_interval(x):
    """
    Map any numeric to [0,1]:
    - x <= 0 -> 0
    - 0 < x <= 1 -> x
    - 1 < x <= 100 -> x/100
    - x > 100 -> 1
    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0.0
    if x <= 0:
        return 0.0
    if x <= 1:
        return x
    if x <= 100:
        return x / 100.0
    return 1.0

def normalize_llm_numbers(llm_obj: dict) -> dict:
    """
    Normalize llm_obj['risk_score'] and llm_obj['confidence'] to [0,1].
    If 'risk_score' missing/zero but 'score' exists, use that.
    Adds 'risk_score_pct' (0–100 float) for convenience.
    """
    if not isinstance(llm_obj, dict):
        return llm_obj

    # risk_score
    rs_raw = llm_obj.get("risk_score")
    if rs_raw in (None, 0, "0", "0.0"):
        # some models return 'score' instead
        alt = llm_obj.get("score")
        if alt is not None:
            rs_raw = alt

    rs = _normalize_unit_interval(_to_float(rs_raw))
    llm_obj["risk_score"] = rs
    llm_obj["risk_score_pct"] = round(rs * 100.0, 2)

    # confidence
    cf_raw = llm_obj.get("confidence")
    cf = _normalize_unit_interval(_to_float(cf_raw))
    llm_obj["confidence"] = cf

    return llm_obj


# ============================================================================
# PROMPT LOADING
# ============================================================================

def _load_prompt(filename: str, required: bool = True) -> Optional[str]:
    """Load a prompt from the prompts directory using relative paths."""
    # Use relative path from this file's location
    prompt_dir = Path(__file__).parent / "prompts"
    prompt_file = prompt_dir / filename
    
    if not prompt_file.exists():
        if required:
            raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
        else:
            logging.warning(f"Optional prompt file not found: {prompt_file}")
            return None
    
    return prompt_file.read_text(encoding='utf-8')


# Load vision prompts from files (required, loaded at module load time)
VISION_SYSTEM_PROMPT = _load_prompt("vision_system_prompt.txt", required=True)
VISION_USER_PROMPT = _load_prompt("vision_user_prompt.txt", required=True)




# ============================================================================
# OCR EXTRACTION & TEXT FILTERING
# ============================================================================




def extract_text_from_screenshot(image_path: str, max_height: int = None, max_pixels: int = None, 
                                   lang: Optional[str] = None) -> Optional[str]:
    """
    Extract text from a screenshot using pytesseract OCR.
    Handles very large/tall images by cropping to reasonable size.
    
    Args:
        image_path: Path to the screenshot image
        max_height: Maximum height in pixels (uses OCR_MAX_HEIGHT from settings if None)
        max_pixels: Maximum total pixels (uses OCR_MAX_PIXELS from settings if None)
        lang: Tesseract language codes (defaults to English if None)
        
    Returns:
        Extracted text, or None if extraction failed
    """
    if max_height is None:
        max_height = OCR_MAX_HEIGHT
    if max_pixels is None:
        max_pixels = OCR_MAX_PIXELS
    try:
        # Open image
        img = Image.open(image_path)
        width, height = img.size
        total_pixels = width * height
        
        # Check if image is too large (potential decompression bomb)
        if FILTER_LARGE_IMAGES and total_pixels > max_pixels:
            logging.warning(f"Image too large: {width}x{height} ({total_pixels:,} pixels > {max_pixels:,}). Skipping OCR.")
            return None
        
        # If image is very tall, crop from top and bottom, keeping middle section
        # (skip header/footer, focus on main content area)
        if FILTER_TALL_IMAGES and height > max_height:
            # Calculate crop region: skip 10% from top, 10% from bottom
            skip_top = int(height * 0.1)
            skip_bottom = int(height * 0.1)
            available_height = height - skip_top - skip_bottom
            
            if available_height > max_height:
                # Take middle portion of max_height
                crop_start = skip_top + (available_height - max_height) // 2
                crop_end = crop_start + max_height
            else:
                # Take whatever is left after skipping top/bottom
                crop_start = skip_top
                crop_end = height - skip_bottom
            
            logging.info(f"Image very tall ({width}x{height}), cropping to middle section: rows {crop_start}-{crop_end}")
            img = img.crop((0, crop_start, width, crop_end))
        
        # Use English as default OCR language
        if lang is None:
            lang = "eng"
        
        # Perform OCR with custom config for better accuracy
        custom_config = r'--oem 1 --psm 6'  # OEM 1 = LSTM, PSM 6 = uniform block of text
        text = pytesseract.image_to_string(img, lang=lang, config=custom_config)
        
        if not text or not text.strip():
            logging.warning(f"No text extracted from {image_path}")
            return None
            
        return text.strip()
        
    except Exception as e:
        logging.error(f"OCR extraction failed for {image_path}: {e}")
        return None


# ============================================================================
# LLM CLIENT
# ============================================================================

def extract_json_from_response(response: str) -> Optional[str]:
    """
    Extract JSON from LLM response, handling markdown code blocks.
    
    Args:
        response: Raw LLM response string
        
    Returns:
        Extracted JSON string, or None if no JSON found
    """
    # First try to find JSON in markdown code blocks
    markdown_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response, re.DOTALL)
    if markdown_match:
        return markdown_match.group(1)
    
    # Fallback: look for raw JSON
    json_match = re.search(r'\{.*\}', response, re.DOTALL)
    if json_match:
        return json_match.group(0)
    
    return None


# Global storage for detailed timing breakdown (for benchmarking)
_detailed_timing = {}

def call_llm_with_image(image_path: str, system_prompt: str, user_prompt: str, 
                         max_height: int = None, max_pixels: int = None) -> Optional[str]:
    """
    Make an LLM call with an image using Ollama's vision capabilities.
    Handles very large/tall images by cropping/resizing to reasonable size.
    
    Args:
        image_path: Path to the image file
        system_prompt: System message for the LLM
        user_prompt: User message for the LLM
        max_height: Maximum height in pixels (uses OCR_MAX_HEIGHT from settings if None)
        max_pixels: Maximum total pixels (uses OCR_MAX_PIXELS from settings if None)
        
    Returns:
        Raw LLM response string, or None if call failed
    """
    if max_height is None:
        max_height = OCR_MAX_HEIGHT
    if max_pixels is None:
        max_pixels = OCR_MAX_PIXELS
    global _detailed_timing
    
    # Detailed timing breakdown
    timing = {
        "image_prep_time": 0.0,
        "http_call_time": 0.0,
        "total_time": 0.0,
        "image_size_bytes": 0,
        "image_size_pixels": 0
    }
    
    total_start = time.time()
    
    try:
        # TIMING: Image preprocessing
        prep_start = time.time()
        
        # Load and validate image size
        img = Image.open(image_path)
        width, height = img.size
        total_pixels = width * height
        timing["image_size_pixels"] = total_pixels
        
        # Check if image is too large
        if FILTER_LARGE_IMAGES and total_pixels > max_pixels:
            logging.warning(f"Image too large for vision: {width}x{height} ({total_pixels:,} pixels). Skipping.")
            return None
        
        # If image is very tall, crop from top and bottom, keeping middle section
        if FILTER_TALL_IMAGES and height > max_height:
            # Calculate crop region: skip 10% from top, 10% from bottom
            skip_top = int(height * 0.1)
            skip_bottom = int(height * 0.1)
            available_height = height - skip_top - skip_bottom
            
            if available_height > max_height:
                # Take middle portion of max_height
                crop_start = skip_top + (available_height - max_height) // 2
                crop_end = crop_start + max_height
            else:
                # Take whatever is left after skipping top/bottom
                crop_start = skip_top
                crop_end = height - skip_bottom
            
            logging.info(f"Image very tall ({width}x{height}), cropping to middle section for vision: rows {crop_start}-{crop_end}")
            img = img.crop((0, crop_start, width, crop_end))
        
        # Convert to bytes
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format='PNG')
        image_bytes = img_byte_arr.getvalue()
        timing["image_size_bytes"] = len(image_bytes)
        
        timing["image_prep_time"] = time.time() - prep_start
        
        # Use threading timeout with progress indication
        result_queue = queue.Queue()
        exception_queue = queue.Queue()
        start_time = time.time()
        
        # TIMING: HTTP call (includes upload + LLM processing + download)
        def llm_call_with_timeout():
            http_start = time.time()
            client = Client(host=_get_llm_host())
            response = client.chat(
                model=_LLM_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user", 
                        "content": user_prompt,
                        "images": [image_bytes]
                    }
                ],
                format="json",  # Force JSON output
                options={
                    "temperature": LLM_TEMPERATURE,
                    "seed": LLM_SEED
                }
            )
            http_time = time.time() - http_start
            return response["message"]["content"], http_time
        
        def worker():
            try:
                result, http_time = llm_call_with_timeout()
                result_queue.put((result, http_time))
            except Exception as e:
                exception_queue.put(e)
        
        thread = threading.Thread(target=worker)
        thread.daemon = True
        thread.start()
        
        # Wait with periodic status updates
        elapsed = 0
        while thread.is_alive() and elapsed < LLM_TIMEOUT:
            thread.join(timeout=10)  # Check every 10 seconds
            elapsed = time.time() - start_time
            if thread.is_alive() and elapsed % 30 < 10:  # Log every 30 seconds
                logging.info(f"Still waiting for LLM vision response... ({int(elapsed)}s / {LLM_TIMEOUT}s)")
        
        if thread.is_alive():
            logging.warning(f"LLM vision call timed out after {LLM_TIMEOUT} seconds")
            timing["total_time"] = time.time() - total_start
            _detailed_timing[image_path] = timing
            return None
        
        if not exception_queue.empty():
            raise exception_queue.get()
        
        if not result_queue.empty():
            result_data = result_queue.get()
            if isinstance(result_data, tuple):
                response_content, http_time = result_data
                if http_time is not None:
                    timing["http_call_time"] = http_time
                timing["total_time"] = time.time() - total_start
                _detailed_timing[image_path] = timing
                return response_content
            else:
                # Fallback for old format
                timing["total_time"] = time.time() - total_start
                _detailed_timing[image_path] = timing
                return result_data
        
        timing["total_time"] = time.time() - total_start
        _detailed_timing[image_path] = timing
        return None
        
    except Exception as e:
        timing["total_time"] = time.time() - total_start
        _detailed_timing[image_path] = timing
        logging.error(f"LLM vision call failed: {e}")
        return None

def get_detailed_timing(image_path: str = None) -> Dict[str, Any]:
    """
    Get detailed timing breakdown for the last call or specific image.
    
    Args:
        image_path: Optional image path to get timing for. If None, returns all timings.
        
    Returns:
        Dict with timing breakdown: image_prep_time, http_call_time, total_time, etc.
    """
    if image_path:
        return _detailed_timing.get(image_path, {})
    return _detailed_timing.copy()


# ============================================================================
# ANALYSIS PIPELINE
# ============================================================================

def analyze_screenshot(screenshot_path: str, verbose: bool = False) -> Dict[str, Any]:
    """
    Full analysis pipeline for screenshot-based sensitivity detection.
    
    FLOW:
    1. ALWAYS run OCR first as a global filter
    2. Check if page is empty/too short
    3. If it fails checks, skip vision LLM analysis
    4. If it passes, run vision-based LLM analysis
    
    Args:
        screenshot_path: Path to screenshot image
        verbose: Print detailed information
        
    Returns:
        Dict with 'ocr_analysis' (for filtering only) and 'vision_analysis' results.
        The 'sensitive' field in combined_verdict indicates the final result.
    """
    start_time = time.time()
    
    # ========================================================================
    # VALIDATE: Check if file exists and is a valid image
    # ========================================================================
    screenshot_file = Path(screenshot_path)
    if not screenshot_file.exists():
        error_msg = f"File does not exist: {screenshot_path}"
        logging.error(error_msg)
        return {
            "ocr_analysis": {
                "llm_raw": None,
                "verdict": {
                    "sensitive": False,
                    "score": 0.0,
                    "source": "file_not_found",
                    "reasons": [error_msg]
                },
                "ocr_text": None,
                "ocr_char_count": 0,
                "processing_time": 0.0
            },
            "vision_analysis": {
                "llm_raw": None,
                "verdict": {
                    "sensitive": False,
                    "score": 0.0,
                    "source": "file_not_found",
                    "reasons": [error_msg]
                },
                "processing_time": 0.0
            },
            "combined_verdict": {
                "sensitive": False,
                "score": 0.0,
                "source": "file_not_found",
                "reasons": [error_msg]
            },
            "total_processing_time": 0.0
        }
    
    # Validate that it's a valid image file
    try:
        test_img = Image.open(screenshot_path)
        test_img.verify()  # Verify it's actually an image (doesn't fully decode, just checks format)
        test_img.close()
        # Reopen since verify() may break the image object
        test_img = Image.open(screenshot_path)
        test_img.load()  # Actually load the image to ensure it's valid
        test_img.close()
    except Exception as e:
        error_msg = f"File is not a valid image or is corrupted: {e}"
        logging.error(f"{error_msg} - {screenshot_path}")
        return {
            "ocr_analysis": {
                "llm_raw": None,
                "verdict": {
                    "sensitive": False,
                    "score": 0.0,
                    "source": "invalid_image",
                    "reasons": [error_msg]
                },
                "ocr_text": None,
                "ocr_char_count": 0,
                "processing_time": 0.0
            },
            "vision_analysis": {
                "llm_raw": None,
                "verdict": {
                    "sensitive": False,
                    "score": 0.0,
                    "source": "invalid_image",
                    "reasons": [error_msg]
                },
                "processing_time": 0.0
            },
            "combined_verdict": {
                "sensitive": False,
                "score": 0.0,
                "source": "invalid_image",
                "reasons": [error_msg]
            },
            "total_processing_time": 0.0
        }
    
    # ========================================================================
    # GLOBAL FILTER: Run OCR first to check if page is worth analyzing
    # ========================================================================
    if verbose:
        logging.info("=" * 80)
        logging.info("GLOBAL FILTER: OCR-based screening")
        logging.info("=" * 80)
        logging.info(f"Extracting text from screenshot: {screenshot_path}")
    
    ocr_start = time.time()
    ocr_text = extract_text_from_screenshot(screenshot_path)
    ocr_time = time.time() - ocr_start
    ocr_char_count = len(ocr_text) if ocr_text else 0
    
    # Check if page passes global filters
    MIN_OCR_CHARS = 10
    skip_all_analysis = False
    skip_reason = None
    
    if not ocr_text:
        skip_all_analysis = True
        skip_reason = "No text could be extracted from screenshot via OCR (valid image but no text found)"
        if verbose:
            logging.warning(f"GLOBAL FILTER: {skip_reason}")
    elif FILTER_MIN_OCR_CHARS and ocr_char_count < MIN_OCR_CHARS:
        skip_all_analysis = True
        skip_reason = f"OCR text too short ({ocr_char_count} characters, minimum {MIN_OCR_CHARS})"
        if verbose:
            logging.warning(f"GLOBAL FILTER: {skip_reason}")
    else:
        if verbose:
            logging.info(f"GLOBAL FILTER PASSED: {ocr_char_count} characters extracted")
            logging.debug(f"First 500 chars: {ocr_text[:500]}")
    
    # If global filter failed, skip all analysis
    if skip_all_analysis:
        if verbose:
            logging.warning("Skipping all LLM analysis due to global filter")
        
        ocr_analysis = {
            "llm_raw": None,
            "verdict": {
                "sensitive": False,
                "score": 0.0,
                "source": "global_filter_failed",
                "reasons": [skip_reason]
            },
            "ocr_text": ocr_text if verbose else None,
            "ocr_char_count": ocr_char_count,
            "processing_time": ocr_time
        }
        
        vision_analysis = {
            "llm_raw": None,
            "verdict": {
                "sensitive": False,
                "score": 0.0,
                "source": "global_filter_failed",
                "reasons": [f"Vision analysis skipped: {skip_reason}"]
            },
            "processing_time": 0.0
        }
        
        combined_verdict = {
            "sensitive": False,
            "score": 0.0,
            "source": "global_filter_failed",
            "reasons": [skip_reason]
        }
        
        total_time = time.time() - start_time
        
        if verbose:
            logging.info("=" * 80)
            logging.info("GLOBAL FILTER RESULT: SKIPPED")
            logging.info("=" * 80)
            logging.info(f"Reason: {skip_reason}")
            logging.info(f"Total processing time: {total_time:.2f}s")
        
        return {
            "ocr_analysis": ocr_analysis,
            "vision_analysis": vision_analysis,
            "combined_verdict": combined_verdict,
            "total_processing_time": total_time
        }
    
    # ========================================================================
    # OCR analysis (for filtering only - OCR LLM analysis is deprecated)
    # ========================================================================
    ocr_analysis = {
        "llm_raw": None,
        "verdict": {
            "sensitive": False,
            "score": 0.0,
            "source": "ocr_filter_only",
            "reasons": [f"OCR extracted {ocr_char_count} characters (used for filtering only, LLM analysis deprecated)"]
        },
        "ocr_text": ocr_text if verbose else None,
        "ocr_char_count": ocr_char_count,
        "processing_time": ocr_time
    }
    
    # ========================================================================
    # Vision-based LLM analysis (direct image to LLM)
    # ========================================================================
    if verbose:
        logging.info("=" * 80)
        logging.info("ANALYSIS 2: VISION-BASED (Direct Image)")
        logging.info("=" * 80)
        logging.info("Sending image directly to LLM vision model...")
    
    vision_start = time.time()
    vision_response = call_llm_with_image(screenshot_path, VISION_SYSTEM_PROMPT, VISION_USER_PROMPT)
    vision_time = time.time() - vision_start
    
    vision_json_str = extract_json_from_response(vision_response) if vision_response else None
    
    if vision_json_str:
        try:
            # Parse JSON
            raw_obj = json.loads(vision_json_str)
            
            # Validate with Pydantic schema if available
            if VisionAnalysisResponse:
                try:
                    validated = VisionAnalysisResponse(**raw_obj)
                    # Use model_dump() for Pydantic v2, fall back to dict() for v1
                    vision_llm_obj = validated.model_dump() if hasattr(validated, 'model_dump') else validated.dict()
                except Exception as validation_error:
                    logging.error(f"Vision LLM Pydantic validation error: {validation_error}")
                    logging.error(f"Raw response: {vision_json_str[:500]}")
                    vision_llm_obj = None
            else:
                vision_llm_obj = raw_obj
        except json.JSONDecodeError as e:
            logging.error(f"Vision LLM JSON parsing error: {e}")
            logging.error(f"Raw response: {vision_json_str[:500]}")
            vision_llm_obj = None
    else:
        vision_llm_obj = None
    
    # Build vision verdict
    if not vision_llm_obj:
        vision_verdict = {
            "sensitive": False,
            "score": 0.0,
            "source": "vision_llm_failed",
            "reasons": ["Vision LLM call failed or returned invalid JSON"]
        }
    else:
        v_sensitive = vision_llm_obj.get("sensitive", False)
        vision_verdict = {
            "sensitive": v_sensitive,
            "score": 1.0 if v_sensitive else 0.0,
            "source": "llm_verdict_vision",
            "reasons": vision_llm_obj.get("reasons", []),
            "primary_intent": vision_llm_obj.get("primary_intent"),
            "confidence": vision_llm_obj.get("confidence"),
            "page_type": vision_llm_obj.get("page_type"),
            "pii_types": vision_llm_obj.get("pii_types", []),
            "quoted_evidence": vision_llm_obj.get("quoted_evidence", [])
        }
    
    if verbose:
        logging.info(f"Vision LLM call completed in {vision_time:.2f}s")
        logging.info(f"Vision Verdict: {'SENSITIVE' if vision_verdict['sensitive'] else 'NOT SENSITIVE'}")
    
    vision_analysis = {
        "llm_raw": vision_llm_obj,
        "verdict": vision_verdict,
        "processing_time": vision_time
    }
    
    # ========================================================================
    # FINAL RESULT (vision analysis is the only LLM analysis)
    # ========================================================================
    total_time = time.time() - start_time
    
    combined_verdict = {
        "sensitive": vision_analysis["verdict"]["sensitive"],
        "score": vision_analysis["verdict"]["score"],
        "source": "vision_analysis",
        "reasons": vision_analysis["verdict"]["reasons"]
    }
    
    if verbose:
        logging.info("=" * 80)
        logging.info("FINAL RESULT")
        logging.info("=" * 80)
        logging.info(f"Final: {'SENSITIVE' if combined_verdict['sensitive'] else 'NOT SENSITIVE'}")
        logging.info(f"Total processing time: {total_time:.2f}s")
    
    return {
        "ocr_analysis": ocr_analysis,
        "vision_analysis": vision_analysis,
        "combined_verdict": combined_verdict,
        "total_processing_time": total_time
    }


# ============================================================================
# COMMAND-LINE INTERFACE
# ============================================================================

def main():
    """Command-line interface for screenshot analysis."""
    parser = argparse.ArgumentParser(
        description="Analyze screenshots for sensitive content using OCR and LLM"
    )
    parser.add_argument("screenshot", help="Path to screenshot image (PNG, JPG, etc.)")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Show detailed processing information")
    parser.add_argument("--dump-ocr", action="store_true",
                       help="Print full OCR text")
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Check if screenshot exists
    if not Path(args.screenshot).exists():
        print(f"Error: Screenshot not found: {args.screenshot}")
        sys.exit(1)
    
    print(f"Analyzing screenshot: {args.screenshot}")
    
    # Run analysis
    result = analyze_screenshot(args.screenshot, verbose=args.verbose)
    
    # Print OCR text if requested
    if args.dump_ocr and result.get('ocr_analysis', {}).get('ocr_text'):
        print("\n" + "=" * 80)
        print("FULL OCR TEXT")
        print("=" * 80)
        print(result['ocr_analysis']['ocr_text'])
        print("=" * 80 + "\n")
    
    # Print results
    final = {
        "file": args.screenshot,
        "ocr_analysis": {
            "char_count": result.get('ocr_analysis', {}).get('ocr_char_count', 0),
            "note": "OCR is used for filtering only, not for LLM analysis"
        },
        "vision_analysis": {
            "sensitive": result.get('vision_analysis', {}).get('verdict', {}).get('sensitive', False),
            "reasons": result.get('vision_analysis', {}).get('verdict', {}).get('reasons', []),
            "llm_raw": result.get('vision_analysis', {}).get('llm_raw')
        },
        "combined_verdict": result.get('combined_verdict', {}),
        "processing_time": result.get('total_processing_time', 0.0)
    }
    
    print("\n=== FINAL VERDICT (JSON) ===")
    print(json.dumps(final, ensure_ascii=False, indent=2))
    
    # Friendly summary
    combined = result.get('combined_verdict', {})
    status = "SENSITIVE" if combined.get('sensitive', False) else "NOT SENSITIVE"
    
    print(f"\n{'=' * 80}")
    print(f"Result: {status}")
    print(f"Total processing time: {result.get('total_processing_time', 0.0):.2f}s")
    print('=' * 80)


def analyze_vision_only_with_prompt(image_path: str, prompt_version: str = None, verbose: bool = False) -> Dict[str, Any]:
    """
    Vision-only analysis using the single vision system prompt.
    
    Note: prompt_version parameter is kept for backward compatibility but does not affect
    which prompt is used (only one prompt version exists). The value is logged and returned
    in results for tracking purposes.
    
    Args:
        image_path: Path to the image file
        prompt_version: Kept for backward compatibility (logged but not used for prompt selection)
        verbose: Print detailed information
        
    Returns:
        Dict with vision analysis results
    """
    # Use the single vision system prompt (no versioning)
    vision_system_prompt = VISION_SYSTEM_PROMPT
    
    if verbose:
        if prompt_version:
            logging.info(f"Making vision LLM call (prompt_version parameter '{prompt_version}' ignored - using default prompt)")
        else:
            logging.info("Making vision LLM call with default prompt")
        logging.info(f"Image: {image_path}")
        logging.info(f"Waiting for LLM response (timeout: {LLM_TIMEOUT}s)...")
    
    vision_start = time.time()
    vision_response = call_llm_with_image(image_path, vision_system_prompt, VISION_USER_PROMPT)
    vision_time = time.time() - vision_start
    
    if verbose:
        if vision_response:
            logging.info(f"LLM call completed in {vision_time:.2f}s")
        else:
            logging.warning(f"LLM call failed or timed out after {vision_time:.2f}s")
    
    vision_json_str = extract_json_from_response(vision_response) if vision_response else None
    
    if vision_json_str:
        try:
            raw_obj = json.loads(vision_json_str)
            
            # Validate with Pydantic schema if available
            if VisionAnalysisResponse:
                try:
                    validated = VisionAnalysisResponse(**raw_obj)
                    vision_llm_obj = validated.model_dump() if hasattr(validated, 'model_dump') else validated.dict()
                except Exception as validation_error:
                    logging.error(f"Vision LLM Pydantic validation error: {validation_error}")
                    vision_llm_obj = None
            else:
                vision_llm_obj = raw_obj
                
            # Normalize numbers
            if vision_llm_obj:
                vision_llm_obj = normalize_llm_numbers(vision_llm_obj)
        except json.JSONDecodeError as e:
            logging.error(f"Vision LLM JSON parsing error: {e}")
            vision_llm_obj = None
    else:
        vision_llm_obj = None
    
    # Build result
    if not vision_llm_obj:
        result = {
            "sensitive": False,
            "risk_score": 0.0,
            "risk_score_pct": 0.0,
            "error": "Vision LLM call failed or returned invalid JSON",
            "primary_intent": None,
            "confidence": 0.0,
            "page_type": None,
            "pii_types": [],
            "quoted_evidence": [],
            "reasons": ["Vision LLM call failed or returned invalid JSON"]
        }
    else:
        result = {
            "sensitive": vision_llm_obj.get("sensitive", False),
            "risk_score": vision_llm_obj.get("risk_score", 0.0),
            "risk_score_pct": vision_llm_obj.get("risk_score_pct", 0.0),
            "error": None,
            "primary_intent": vision_llm_obj.get("primary_intent"),
            "confidence": vision_llm_obj.get("confidence", 0.0),
            "page_type": vision_llm_obj.get("page_type"),
            "pii_types": vision_llm_obj.get("pii_types", []),
            "quoted_evidence": vision_llm_obj.get("quoted_evidence", []),
            "reasons": vision_llm_obj.get("reasons", []),
            "prompt_version": prompt_version
        }
    
    result["processing_time"] = vision_time
    result["llm_raw"] = vision_llm_obj
    
    return result


if __name__ == "__main__":
    main()

