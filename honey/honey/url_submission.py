from collections import deque
from itertools import permutations, product
import random
import shutil
import dotenv
import time
import logging
import httpx
import os
import json
import traceback
import datetime
from abc import ABC, abstractmethod
from honey.submitters import Radar, URLScan, Service, URLQuery, AnyRun, JoeSandbox, HybridAnalysis
import qrcode
from config.settings import DISCORD_WEBHOOK_URL as WEBHOOK_URL


dotenv.load_dotenv()
USE_DYNAMIC_SUBDOMAIN = os.getenv("USE_DYNAMIC_SUBDOMAIN", "false").lower() == "true"
ARTIFACT_BASE_DIR = os.getenv("ARTIFACT_BASE_DIR", "site/base")
services: list[Service] = [HybridAnalysis(), JoeSandbox()] 

def notify_error(e: Exception) -> None:
    webhook_url = WEBHOOK_URL
    data = {"content": ''.join(traceback.format_exception_only(type(e), e))}
    response = httpx.post(webhook_url, json=data)
    if response.status_code != 204:
        logging.error(f"Failed to send Discord alert: {response.text}")
    else:
        logging.info("Discord alert sent successfully.")

def save_results(data: dict):
    with open("data/url_submissions.jsonl", "a") as f:
        f.write(json.dumps(data) + "\n")

def submit_url(url: str, s: Service, vis: str, sub: str, url_type: str):
    res_url = error = None
    try:
        res = s.submit(url=url, visibility_type=vis, submission_type=sub)
        if isinstance(res, str):
            logging.info(f"[{s.name[0]}] successfully submitted: {res}") 
            res_url = res
        elif isinstance(res, Exception):
            logging.info(f"[{s.name[0]}] {res}")
            notify_error(res)
            error = res
        else:
            raise Exception(f"[{s.name[0]}] Invalid result type: {res}")
    except Exception as e:
        logging.exception(f"[{s.name[0]}] Major Exception: {e}")
        notify_error(e)
        error = e
    finally:
        # Save results
        save_results({"Submission Time": datetime.datetime.now().isoformat(), "Service": s.name[0], "Visibility": vis, "Submission Type": sub, "URL Type": url_type, "Submission URL": url, "Result URL": res_url, "Error": str(error)})
        logging.info(f"[{s.name[0]}] Waiting for {s.rate_limit} seconds")
        time.sleep(s.rate_limit)

def create_qr_code(qr_code_path: str, qr_code_contained_url: str):
    """
    Generate a QR code for a given URL and save it to the specified path.

    - Generates a QR code image for the provided URL.
    - Saves the image to the output path.
    - Copies the QR code image to the account files directory.
    """
    try:
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L, # type: ignore
            box_size=10,
            border=2,
        )
        qr.add_data(qr_code_contained_url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        img.save(qr_code_path) # type: ignore
    except Exception as e:
        logging.exception(f"[MAIN] failed creation of QR Code: {e}")
        notify_error(Exception("Creation of QR code failed"))
        raise e

def create_repo(path: str, source: str):
    try:
        # Copy the base dir
        shutil.copytree(source, path, dirs_exist_ok=True)
    except Exception as e:
        logging.exception(f"[MAIN] failed creation of {path}: {e}")
        notify_error(Exception("Creation of URL repo failed"))
        raise e
    

class URLType(ABC):
    base_url = "https://sheep-savvy.com/"
    name = "URLType"
    @abstractmethod
    def create_url(self, sub_id: int, vis: str, sub: str, name: str) -> str:
        pass

    def setup_repo(self, path: str) -> None:
        """
        Create the files such that they can be served by Caddy.
        We copy the base-page to the correct location, then we change the QR code contained.
        All other tokens are left at their default values (apr)
        """
        OUTPUT_BASE_DIR = "site/"
        files_dir = os.path.join(OUTPUT_BASE_DIR, path)
        create_repo(files_dir, ARTIFACT_BASE_DIR)
        qr_code_path = os.path.join(files_dir, "qr.png")
        qr_code_contained_url = f"{self.base_url}{path}/qr-info.html" # Does not exist
        create_qr_code(qr_code_path, qr_code_contained_url)

class BoringStatic(URLType):
    """BoringStatic has "news" in the URL and other than that only the required identification parameters
    """
    name = "BoringStatic"
    def create_url(self, sub_id: int, vis: str, sub: str, name: str) -> str:        
        path = f"news/{sub_id}/{name}{vis}{sub}"
        super().setup_repo(path)
        return f"{self.base_url}{path}/"
    
class InterestingStatic(URLType):
    """InterestingStatic has "secret/login" in the URL and other than that only the required identification parameters
    """
    name = "InterestingStatic"
    def create_url(self, sub_id: int, vis: str, sub: str, name: str) -> str:        
        path = f"secret/login/{sub_id}/{name}{vis}{sub}"
        super().setup_repo(path)
        return f"{self.base_url}{path}/"
    
class EntropyStaticPath(URLType):
    """EntropyStaticPath has "secret/login/562210be-067c-4a62-b8a8-df27f3893a80" in the URL and other than that only the required identification parameters
    """

    name = "EntropyStaticPath"
    def create_url(self, sub_id: int, vis: str, sub: str, name: str) -> str:        
        path = f"secret/login/562210be-067c-4a62-b8a8-df27f3893a80/{sub_id}/{name}{vis}{sub}"
        super().setup_repo(path)
        return f"{self.base_url}{path}/"
    
class EntropyStaticQuery(URLType):
    """EntropyStaticQuery has "dashboard" in the URL and "?authCode=7e32a729b1226ed1270f282a8c63054d09b26bc9ec53ea69771ce38158dfade8&key=U2VjcmV0S2V5MTIzNDU2Nzg5MA==" in the query
    """
    name = "EntropyStaticQuery"
    def create_url(self, sub_id: int, vis: str, sub: str, name: str) -> str:        
        path = f"dashboard/{sub_id}/{name}{vis}{sub}"
        super().setup_repo(path)
        return f"{self.base_url}{path}/?authCode=7e32a729b1226ed1270f282a8c63054d09b26bc9ec53ea69771ce38158dfade8&key=U2VjcmV0S2V5MTIzNDU2Nzg5MA=="


def main():

    num_submissions = int(os.getenv("NUM_SUBMISSIONS", 24))
    wait_seconds = int(os.getenv("WAIT_SECONDS", 60*60*11)) # Wait for 11 hours between submissions (+ time taken for the submissions)
    url_types = deque(list(permutations([BoringStatic(), InterestingStatic(), EntropyStaticPath(), EntropyStaticQuery()])))
    
    for submission_id in range(1, num_submissions + 1):
        if USE_DYNAMIC_SUBDOMAIN:
            URLType.base_url = f"https://{submission_id}.sheep-savvy.com/"
        else:
            URLType.base_url = "https://3.sheep-savvy.com/"
        for s in services:
            logging.debug(f"[{s.name[0]}] {s.name} {s.submission_types} {s.visibility_types}")
            for ((vis_desc, vis), (sub_desc, sub)) in s.submission_options[0]:
                for url_type in url_types[0]:
                    url = url_type.create_url(submission_id, vis, sub, s.name[1])
                    logging.info(f"[{s.name[0]}] Submitting {url} with {vis_desc} and {sub_desc}")
                    submit_url(url=url, s=s, vis=vis, sub=sub, url_type=url_type.name)
            # Change the order of the submission options for the next submission
            s.submission_options.rotate(1)
        
        # Wait for the next submission
        logging.warning(f"[MAIN] sleeping for {wait_seconds} seconds")
        time.sleep(wait_seconds)
        # Change the order of the url_types for the next submission
        url_types.rotate(1)



if __name__ == "__main__":
    # Define format
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    formatter = logging.Formatter(log_format)

    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler("honey/logs/url_submission.log", mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream handler (console)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
   
    # Create the permutations of submission options
    random.seed(42)
    for s in services:
        submission_options = list(product(s.visibility_types, s.submission_types))
        if len(submission_options) > 6:
            raise Exception(f"Too many submission options: {s.name[0]}, {len(submission_options)}")
        submission_permutations = list(permutations(submission_options))
        random.shuffle(submission_permutations)
        s.submission_options = deque(submission_permutations)
        
    # Start the program
    main()