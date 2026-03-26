import re
from urllib.parse import urlparse
from typing import Optional
import requests
import time
import logging
from config.settings import MODEL_SERVER, BASE_SNAPSHOT_DIR
import json
import psutil
import os

class UUIDExtractor:
    """Handles UUID extraction from different service URLs consistently."""
    
    # Service-specific patterns
    PATTERNS = {
        'urlscan': r"/result/([a-f0-9\-]+)/?$",
        'cloudflare': r"/(?:scan|url-scanner)/([a-f0-9\-]+)",
        'anyrun': r"/tasks/([a-f0-9\-]+)",
        'hybrid_analysis': r"/sample/([a-f0-9\-]+)",
        'urlquery': r"/analysis/([a-f0-9\-]+)",
        'joe': r"/analysis/(\d+)"
    }
    
    @classmethod
    def extract_uuid(cls, result_url: str, service: str) -> Optional[str]:
        """
        Extract UUID from a result URL for a specific service.
        
        Args:
            result_url: The result URL from the service
            service: The service name (must be one of the keys in PATTERNS)
            
        Returns:
            The extracted UUID if found, None otherwise
        """
        if service not in cls.PATTERNS:
            raise ValueError(f"Unsupported service: {service}")
            
        pattern = cls.PATTERNS[service]
        match = re.search(pattern, result_url)
        return match.group(1) if match else None
    
    @classmethod
    def get_snapshot_path(cls, uuid: str, service: str, timestamp: str) -> dict:
        """
        Generate consistent snapshot paths for a given UUID and service.
        
        Args:
            uuid: The extracted UUID
            service: The service name
            timestamp: Timestamp string for the snapshot
            
        Returns:
            Dictionary containing paths for screenshots and DOM files
        """
        base_dir = os.path.join(BASE_SNAPSHOT_DIR, service, uuid, timestamp)
        
        return {
            "screenshot_before": f"{base_dir}/before.png",
            "screenshot_after": f"{base_dir}/after.png",
            "dom_before": f"{base_dir}/before.html",
            "dom_after": f"{base_dir}/after.html"
        } 

last_model_alert = 0
MODEL_ALERT_INTERVAL = MODEL_SERVER['alert_interval']

def is_ollama_running():
    """Check if Ollama server is running by hitting the root endpoint."""
    try:
        resp = requests.get(f"http://{MODEL_SERVER['host']}:{MODEL_SERVER['port']}/", timeout=2)
        if resp.status_code == 200 and "Ollama is running" in resp.text:
            return True
        return False
    except Exception:
        return False

def is_model_ready(model_name):
    """Check if the Ollama model is loaded and ready by sending a real inference request."""
    if not is_ollama_running():
        logging.debug("Ollama server not running on root endpoint")
        return "port_unavailable"

    url = f"http://{MODEL_SERVER['host']}:{MODEL_SERVER['port']}{MODEL_SERVER['endpoint']}"
    try:
        resp = requests.post(
            url,
            json={"model": model_name, "prompt": "ping"},
            timeout=MODEL_SERVER['timeout']
        )
    except requests.exceptions.ConnectionError:
        return "port_unavailable"
    except requests.exceptions.ReadTimeout:
        logging.debug("ReadTimeout: Ollama server timed out")
        return "model_not_loaded"
    except Exception as e:
        logging.debug(f"Exception during request: {e}")
        return "unknown_error"

    # Check status code first
    if resp.status_code >= 400:
        try:
            # Try to parse error JSON
            lines = resp.content.decode().splitlines()
            for line in lines:
                try:
                    data = json.loads(line)
                    if 'error' in data and 'model' in data['error'].lower():
                        return "model_not_loaded"
                except Exception as e:
                    logging.debug(f"Exception decoding NDJSON line: {e}")
                    continue
            return "unknown_error"
        except Exception as e:
            logging.debug(f"Exception processing NDJSON: {e}")
            logging.debug(f"Response content: {resp.content}")
            return "unknown_error"

    # For HTTP 200, look for 'done': true
    try:
        lines = resp.content.decode().splitlines()
        for line in lines:
            try:
                data = json.loads(line)
                if 'error' in data and 'model' in data['error'].lower():
                    return "model_not_loaded"
                if data.get('done', False):
                    return "ready"
            except Exception as e:
                logging.debug(f"Exception decoding NDJSON line: {e}")
                continue
        return "unknown_error"
    except Exception as e:
        logging.debug(f"Exception processing NDJSON: {e}")
        logging.debug(f"Response content: {resp.content}")
        return "unknown_error"

def send_model_unavailable_alert(status):
    """Send a Discord alert about model/port status, rate-limited, with specific message."""
    global last_model_alert
    now = time.time()
    if now - last_model_alert > MODEL_ALERT_INTERVAL:
        if status == "port_unavailable":
            msg = "Ollama server (port 51000) is not reachable!"
        elif status == "model_not_loaded":
            msg = "Ollama server is up, but the model is not loaded!"
        else:
            msg = "Model on port 51000 is unavailable or not loaded! Pipeline is paused."
        last_model_alert = now 

def get_chrome_children(parent_pid):
    """Return a list of psutil.Process objects for Chrome children of the given parent PID (recursive)."""
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return []
    chrome_children = []
    for child in parent.children(recursive=True):
        try:
            if 'chrome' in child.name().lower():
                chrome_children.append(child)
        except psutil.NoSuchProcess:
            continue
    return chrome_children 

def get_snapshot_paths(service_name: str, url_id: str, timestamp: str) -> dict:
    """
    Generate consistent snapshot paths for a given UUID and service.
    Args:
        service_name: The service name
        url_id: The extracted UUID
        timestamp: Timestamp string for the snapshot
    Returns:
        Dictionary containing paths for screenshots, DOM files, and HAR files
    """
    base_dir = os.path.join(BASE_SNAPSHOT_DIR, service_name, url_id, timestamp)
    return {
        "screenshot_before": f"{base_dir}/before.png",
        "screenshot_after": f"{base_dir}/after.png",
        "dom_before": f"{base_dir}/before.html",
        "dom_after": f"{base_dir}/after.html",
        "har_before": f"{base_dir}/before.har",
        "har_after": f"{base_dir}/after.har"
    } 
