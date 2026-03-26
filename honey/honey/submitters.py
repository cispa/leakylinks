from collections import deque
import re
import httpx
import dotenv
import os
from abc import ABC, abstractmethod
import logging
from patchright.sync_api import sync_playwright, Page
import time
import json
import requests

dotenv.load_dotenv()

class Service(ABC):
    """Abstract service class"""
    submission_options = deque()
    @property
    @abstractmethod
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        pass

    @property
    @abstractmethod
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {}

    @property
    @abstractmethod
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        pass

    @property
    @abstractmethod
    def rate_limit(self) -> int:
        """How long to wait between each submission (for each submission type?)"""
        pass

    @abstractmethod
    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        """
        Submit the given URL with the chosen visibility and submission type.
        """
        if not any(visibility_type == desc for _, desc in self.visibility_types):
            raise ValueError(f"Invalid visibility type: {visibility_type}")
        if not any(submission_type == desc for _, desc in self.submission_types):
            raise ValueError(f"Invalid submission type: {submission_type}")
        logging.info(f"[{self.name[0]}] Submitting {url} via {submission_type} with {self.vis_dict[visibility_type]} visibility.")


class URLScan(Service):
    """Implementation for URLscan"""
    @property
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        return ("URLScan", "u")

    @property
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {
            "pu": "Public",
            "pr": "Private",
            "u": "Unlisted"
        }
        return [("public", "pu"), ("private", "pr"), ("unlisted", "u")]

    @property
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        return [("api", "a"), ("website", "w")]
    
    @property
    def rate_limit(self) -> int:
        """How long to wait between each submission (for each submission type?)"""
        # Increased to 60s to avoid hitting URLScan rate limits
        return 60

    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        super().submit(url, visibility_type, submission_type)
        
        if submission_type == "w":
            return self.submit_website(url, visibility_type)
        elif submission_type == "a":
            return self.submit_api(url, visibility_type)
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")
    
    def check_login(self, page: Page) -> bool:
        page.goto("https://urlscan.io/user/profile/")
        return page.url == "https://urlscan.io/user/profile/" 
    
    def login(self, page: Page) -> bool:
        page.goto("https://urlscan.io/user/login/")
        page.get_by_role("textbox", name="Your e-mail address").click()
        page.get_by_role("textbox", name="Your e-mail address").fill(os.getenv("URLSCAN_EMAIL", "invalid"))
        page.get_by_role("textbox", name="Your e-mail address").press("Tab")
        page.get_by_role("textbox", name="Your password").fill(os.getenv("URLSCAN_PASSWORD", "invalid"))
        page.get_by_role("button", name="Login").click()
        return self.check_login(page)

    def submit_website(self, url: str, visibility_type: str) -> str | Exception:
        with sync_playwright() as playwright:
            try:
                context = playwright.chromium.launch_persistent_context(user_data_dir="data_dirs/urlscan", channel="chrome", headless=False, viewport={'width': 1920, 'height': 1080})
                page = context.new_page()
                logging.info(f"[{self.name[0]}] Checking if still logged in")
                if not self.check_login(page):
                    logging.info(f"[{self.name[0]}] Not logged in anymore, try to login again")
                    if not self.login(page):
                        raise Exception("Login Failed!")
                logging.info(f"[{self.name[0]}] Still logged in")
                    
                page.goto("https://urlscan.io/")
                page.get_by_role("textbox", name="URL to scan").click()
                page.get_by_role("textbox", name="URL to scan").fill(url)
                page.locator("a").filter(has_text="Options").click()
                mode = self.vis_dict[visibility_type]
                page.locator("span").filter(has_text=re.compile(rf"^{mode}$")).first.click()
                page.get_by_role("button", name=f"{mode} Scan").click() # Requires high enough resolution of the screen
                page.get_by_role("button", name="Add Verdict").click(timeout=60000)
                page.get_by_role("button", name="Back to summary").click()
                return page.url
            except Exception as e:
                logging.exception(f"[{self.name[0]}] Major submission error!: {e}")
                return e
            finally:
                try:
                    context.close() # type: ignore
                except Exception as e:
                    logging.exception(f"[{self.name[0]}] Closing browser failed: {e}")
                
    def submit_api(self, url: str, visibility_type: str) -> str | Exception:
        """Submit url with visibility_type via API: requires an API key"""
        headers = {"API-KEY": os.getenv("URLSCAN_APIKEY", "NOKEY")}
        data = {"url": url, "visibility": self.vis_dict[visibility_type].lower()}
        try:
            response = httpx.post('https://urlscan.io/api/v1/scan/', json=data, headers=headers)
            res = response.json()
            try:
                return res["result"]
            except KeyError:
                logging.exception(f"[{self.name[0]}] URLScan error, URL not submitted correctly!")
                return Exception(res)
        except httpx.RequestError as e:
            logging.exception(f"[{self.name[0]}] Major submission error!: {e}")
            return e


class Radar(Service):
    """Implementation for Cloudflare Radar"""
    @property
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        return ("Cloudflare Radar", "r")

    @property
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {
            "pu": "Public",
            "u": "Unlisted" # Note that unlisted for radar is equivalent to private for URLscan
        }
        return [("public", "pu"), ("unlisted", "u")]

    @property
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        return [("api", "a"), ("website", "w")]
    
    @property
    def rate_limit(self) -> int:
        """How long to wait between each submission (for each submission type?)"""
        # Limit is 10s https://developers.cloudflare.com/security-center/investigate/scan-limits/
        # However, the same domain (website was recently scanned) seems to be only scannable every 60s
        return 60
    
    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        super().submit(url, visibility_type, submission_type)
        self.cloudflare_account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID", "INVALID")
        
        if submission_type == "w":
            return self.submit_website(url, visibility_type)
        elif submission_type == "a":
            return self.submit_api(url, visibility_type)
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")
        
    def check_login(self, page: Page) -> bool:
        home_url = "https://dash.cloudflare.com/profile/managed-profile/preferences"
        page.goto(home_url)
        try:
            page.get_by_text("Dashboard appearance").click()
            return True
        except Exception as e:
            logging.exception(f"[{self.name[0]}] Preferences do not exist, likely not logged in anymore: {e}")
            return False
    
    def login(self, page: Page) -> bool:
        raise NotImplementedError("Cloudflare radar login cannot be automated, please login manually via: `DISPLAY=:99 poetry run python honey/login_cloudflare.py`")

    def submit_website(self, url: str, visibility_type: str) -> str | Exception:
        """Submit via website (unlisted requires to be logged in)"""
        with sync_playwright() as playwright:
            try:
                context = playwright.chromium.launch_persistent_context(user_data_dir="data_dirs/radar", channel="chrome", headless=False, no_viewport=True)
                page = context.new_page()
                page.goto(f"https://dash.cloudflare.com/{self.cloudflare_account_id}/security-center/investigate?url={url}")
                if visibility_type == "pu":
                    page.get_by_role("radio", name="Public Info").check()
                elif visibility_type == "u":
                    page.get_by_role("radio", name="Unlisted Info").check()
                else:
                    raise Exception(f"Invalid visibility type: {visibility_type}")
                
                page.get_by_role("button", name="Scan now").click()
                # The URL update takes some time, click on a button that only appears after the scan finished to make sure the URL is correct
                page.get_by_role("button", name="Related Scans").click(timeout=60000)
                res = page.url
                try:
                    url_report = httpx.URL(res).params["url-report"]
                    return f"https://radar.cloudflare.com/scan/{url_report}/summary"
                except KeyError:
                    raise Exception(f"Submission likely failed, no url-report in URL: {res}")
            except Exception as e:
                logging.exception(f"[{self.name[0]}] Major submission error!: {e}")
                return e
            finally:
                try:
                    context.close() # type: ignore
                except Exception as e:
                    logging.exception(f"[{self.name[0]}] Closing failed: {e}")
    
    def submit_api(self, url: str, visibility_type: str) -> str | Exception:
        """Submit url with visibility_type via API: requires an API key
            Notes:
            - 10s wait between each request due to API ratelimiting
            - 60s wait between request to the same domain limiting (recently scanned website; seems broken via website this only applies to the exact same URL and not to the domain)
        """
        headers = {"Authorization": "Bearer " + os.getenv("CLOUDFLARE_API_KEY", "NOKEY")}
        data = {"url": url, "visibility": self.vis_dict[visibility_type]}
        try:
            response = httpx.post(f'https://api.cloudflare.com/client/v4/accounts/{self.cloudflare_account_id}/urlscanner/v2/scan', json=data, headers=headers)
            res = response.json()
            try:
                return f"https://radar.cloudflare.com/scan/{res['uuid']}/summary"
            except KeyError:
                logging.exception(f"[{self.name[0]}] URL not submitted correctly!")
                return Exception(res)
        except httpx.RequestError as e:
            logging.exception(f"[{self.name[0]}] Major submission error!: {e}")
            return e


class URLQuery(Service):
    """Implementation for URLquery.net"""
    @property
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        return ("URLQuery", "q") # U is already taken by URLscan

    @property
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {
            "pu": "Public",
            "pr": "Private", 
            "re": "Restricted"
        }
        return [("public", "pu"), ("private", "pr"), ("restricted", "re")]

    @property
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        return [("api", "a"), ("website", "w")]
    
    @property
    def rate_limit(self) -> int:
        """How long to wait between each submission"""
        return 300  # Increased to 5 minutes to avoid rate limiting
    
    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        super().submit(url, visibility_type, submission_type)
        
        if submission_type == "w":
            return self.submit_website(url, visibility_type)
        elif submission_type == "a":
            return self.submit_api(url, visibility_type)
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")
    
    def submit_api(self, url: str, visibility_type: str) -> str | Exception:
        """Submit via API"""
        try:
            api_url = "https://api.urlquery.net/public/v1/submit/url"
            api_key = os.getenv("URLQUERY_API_KEY")
            # Avoid logging raw environment values
            headers = {
                'accept': 'application/json',
                'x-apikey': api_key,
                'Content-Type': 'application/json'
            }
            
            # Map visibility types to API access levels
            # Temporarily use "public" for all types since private/restricted are failing
            access_mapping = {
                "pu": "public",
                "pr": "private", 
                "re": "restricted"
            }
            
            payload = {
                "url": url,
                "useragent": "Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0",
                "referer": "",
                "access": access_mapping.get(visibility_type, "public")
            }
            
            # Debug logging (no secrets)
            logging.info(f"[{self.name[0]}] Using URLQuery API submission")
            logging.info(f"[{self.name[0]}] Payload: {{'url': 'REDACTED', 'access': payload.get('access')}}")
            
            response = requests.post(api_url, headers=headers, json=payload)
            
            # Debug logging
            logging.info(f"[{self.name[0]}] Response Status: {response.status_code}")
            logging.info(f"[{self.name[0]}] Response Headers: {dict(response.headers)}")
            logging.info(f"[{self.name[0]}] Response Text: {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                logging.info(f"[{self.name[0]}] API submission successful: {result.get('report_id', 'unknown')}")
                return f"https://urlquery.net/report/{result.get('report_id', '')}"
            else:
                error_msg = f"API submission failed with status {response.status_code}: {response.text}"
                logging.error(f"[{self.name[0]}] {error_msg}")
                return Exception(error_msg)
                
        except Exception as e:
            logging.exception(f"[{self.name[0]}] API submission error: {e}")
            return e
    
    def submit_website(self, url: str, visibility_type: str) -> str | Exception:
        """Submit via website using saved login profile"""
        with sync_playwright() as playwright:
            try:
                context = playwright.chromium.launch_persistent_context(user_data_dir="data_dirs/urlquery", channel="chrome", headless=False, no_viewport=True)
                page = context.new_page()
                
                # Go to URLQuery main page
                page.goto("https://urlquery.net")
                time.sleep(3)
                
                # Fill in the URL
                page.get_by_role("textbox", name="Submit URL (e.g. urlquery.net)").click()
                time.sleep(1)
                page.get_by_role("textbox", name="Submit URL (e.g. urlquery.net)").fill(url)
                time.sleep(2)
                
                # Select visibility mode based on visibility_type
                if visibility_type == "pu":
                    # Click on Public
                    page.get_by_role("heading", name="Public", exact=True).click()
                    time.sleep(1)
                elif visibility_type == "pr":
                    # Click on Private
                    page.get_by_role("heading", name="Private").click()
                    time.sleep(1)
                elif visibility_type == "re":
                    # Click on Restricted
                    page.get_by_role("heading", name="Restricted").click()
                    time.sleep(1)
                
                # Submit the URL
                page.get_by_role("button", name="Submit").click()
                time.sleep(2)
                
                # Wait for either queue or report page to load
                try:
                    page.wait_for_url("**/report/**", wait_until='commit', timeout=60000)
                except:
                    # If report page doesn't load, try queue page
                    page.wait_for_url("**/queue/**", wait_until='commit', timeout=60000)
                
                return page.url
                
            except Exception as e:
                logging.exception(f"[{self.name[0]}] Major submission error!: {e}")
                return e
            finally:
                try:
                    context.close() # type: ignore
                except Exception as e:
                    logging.exception(f"[{self.name[0]}] Closing failed: {e}")

class AnyRun(Service):
    """Implementation for Any.Run"""
    @property
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        return ("AnyRun", "a") 

    @property
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {
            "pu": "Public", # Maybe there is another option? Check again after login works
        }
        return [("public", "pu")]

    @property
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        return [("website", "w")] # API is not available in the free version?
    
    @property
    def rate_limit(self) -> int:
        """How long to wait between each submission"""
        return 300 # 5 minutes between scans
    
    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        super().submit(url, visibility_type, submission_type)
        
        if submission_type == "w":
            return self.submit_website(url, visibility_type)
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")
        
    def check_login(self, page: Page) -> bool:
        """Check if already logged in"""
        try:
            page.goto("https://app.any.run/dashboard")
            # Check if we're redirected to login or stay on dashboard
            return page.url == "https://app.any.run/dashboard"
        except:
            return False
    
    def login(self, page: Page) -> bool:
        """Login to AnyRun"""
        try:
            page.goto('https://app.any.run/')
            
            # Click Sign In
            page.locator('#modalLogIn').get_by_text('Sign In').click()
            
            # Fill in credentials
            page.get_by_role('textbox', { name: 'Business Email' }).fill(os.getenv("ANYRUN_EMAIL", ""))
            page.get_by_role('textbox', { name: 'Password' }).fill(os.getenv("ANYRUN_PASSWORD", ""))
            
            # Click Sign in
            page.get_by_role('button', { name: 'Sign in', exact: true }).click()
            
            # Wait for login to complete (check if we're on the dashboard)
            page.wait_for_url('**/dashboard**', timeout=10000)
            
            return True
        except Exception as e:
            logging.error(f"[{self.name[0]}] Login failed: {e}")
            return False
    
    def submit_website(self, url: str, visibility_type: str) -> str | Exception:
        """Submit via website (requires to be logged in)"""
        try:
            with sync_playwright() as p:
                context = p.chromium.launch_persistent_context(user_data_dir="data_dirs/anyrun", channel="chrome", headless=False, no_viewport=True)
                page = context.new_page()
                
                # Check if already logged in, if not login
                if not self.check_login(page):
                    if not self.login(page):
                        context.close()
                        return Exception("Failed to login to AnyRun")
                
                # Navigate to main page and wait
                page.goto('https://app.any.run/')
                page.wait_for_load_state('networkidle', timeout=60000)  # Increased timeout to 60 seconds
                time.sleep(2)
                
                # Click on "New analysis" button
                page.get_by_role('button', name='New analysis').click()
                page.wait_for_load_state('networkidle', timeout=60000)  # Increased timeout to 60 seconds
                time.sleep(2)
                
                # Click on the URL block content
                page.locator('.url-block__content').click()
                time.sleep(1)
                
                # Click on the URL textbox and fill it
                url_input = page.get_by_role('textbox', name='URL here')
                url_input.click()
                url_input.fill(url)
                
                # Wait for URL to be processed and button to become available
                run_button = page.get_by_role('button', name='Run a public analysis')
                
                # Wait for the button to be enabled (URL validation complete)
                max_wait = 30  # Maximum 30 seconds to wait
                for i in range(max_wait):
                    if run_button.is_enabled():
                        break
                    time.sleep(1)
                    if i % 5 == 0:  # Log every 5 seconds
                        logging.info(f"[{self.name[0]}] Waiting for URL validation... ({i+1}s)")
                
                if not run_button.is_enabled():
                    logging.error(f"[{self.name[0]}] Run button still disabled after {max_wait} seconds")
                    return Exception("Run button is disabled - URL validation failed or took too long")
                
                # Click the button
                run_button.click()
                time.sleep(2)
                
                # Handle the popup dialog
                try:
                    # Click "Don't show on this week"
                    page.get_by_text("Don't show on this week").click()
                    time.sleep(1)
                    
                    # Click "I Agree"
                    page.get_by_role('button', name='I Agree').click()
                    time.sleep(2)
                except:
                    # If popup doesn't appear, continue
                    pass
                
                # Wait for submission to complete and get the result URL
                # The result page should have a URL like https://app.any.run/tasks/...
                try:
                    page.wait_for_url('**/tasks/**', timeout=60000)  # Increased timeout to 60 seconds
                    page.wait_for_load_state('networkidle', timeout=60000)  # Increased timeout to 60 seconds
                    time.sleep(3)
                except Exception as e:
                    # If navigation doesn't happen, check if we're still on the same page
                    current_url = page.url
                    logging.error(f"[{self.name[0]}] Navigation failed. Current URL: {current_url}")
                    
                    # Check if there are any error messages on the page
                    try:
                        error_text = page.locator('.error-message, .alert, .notification').text_content()
                        if error_text:
                            logging.error(f"[{self.name[0]}] Error message found: {error_text}")
                    except:
                        pass
                    
                    return Exception(f"Analysis submission failed - no navigation to tasks page. Current URL: {current_url}")
                
                result_url = page.url
                context.close()
                
                return result_url
                
        except Exception as e:
            logging.exception(f"[{self.name[0]}] Website submission failed: {e}")
            return Exception(f"Website submission failed: {e}")

class HybridAnalysis(Service):
    """Implementation for HybridAnalysis"""
    @property
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        return ("HybridAnalysis", "h")

    @property
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {
            "pu": "Public", 
        }
        return [("public", "pu")]

    @property
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        return [("api", "a")]
    
    @property
    def rate_limit(self) -> int:
        """How long to wait between each submission in seconds"""
        return 60*60 
    
    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        super().submit(url, visibility_type, submission_type)
        
        if submission_type == "w":
            return self.submit_website(url, visibility_type)
        elif submission_type == "a":
            return self.submit_api(url, visibility_type)
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")
        
    def submit_website(self, url: str, visibility_type: str) -> str | Exception:
        """Submit via website (cannot be automated due to "I'm not a robot"?)"""
        raise NotImplementedError("Requires solving I'm not a robot for each submission, even when logged in")        
    
    def submit_api(self, url: str, visibility_type: str) -> str | Exception:
        """Submit url with visibility_type via API: requires an API key"""
        headers = {"api-key": os.getenv("HYBRID_API_KEY", "NOKEY")}
        data = {"url": url, "environment_id": "160"} 
        api_url = "https://hybrid-analysis.com/api/v2/submit/url"

        try:
            # Follow redirects automatically with longer timeout
            response = httpx.post(api_url, data=data, headers=headers, follow_redirects=True, timeout=60.0)
            
            # Check if the response is successful (200 OK or 201 Created)
            if response.status_code not in [200, 201]:
                logging.error(f"[{self.name[0]}] API request failed with status {response.status_code}: {response.text}")
                return Exception(f"API request failed with status {response.status_code}: {response.text}")
            
            # Try to parse JSON response
            try:
                res = response.json()
            except Exception as e:
                logging.error(f"[{self.name[0]}] Failed to parse JSON response: {e}")
                logging.error(f"[{self.name[0]}] Response content: {response.text}")
                return Exception(f"Failed to parse JSON response: {e}")
            
            try:
                return f"https://www.hybrid-analysis.com/sample/{res['sha256']}"
            except KeyError:
                logging.exception(f"[{self.name[0]}] URL not submitted correctly!")
                return Exception(res)
        except httpx.TimeoutException as e:
            logging.error(f"[{self.name[0]}] Request timed out: {e}")
            return Exception(f"Request timed out: {e}")
        except httpx.RequestError as e:
            logging.exception(f"[{self.name[0]}] Major submission error!: {e}")
            return e        

class JoeSandbox(Service):
    """Implementation of JoeSandbox"""
    @property
    def name(self) -> tuple[str, str]:
        """Name of the service: (long name, short name)"""
        return ("JoeSandbox", "j") 

    @property
    def visibility_types(self) -> list[tuple[str, str]]:
        """List of supported visibility types: List[(long name, short name)]"""
        self.vis_dict = {
            "pu": "Public", # Private available for pro users
        }
        return [("public", "pu")]

    @property
    def submission_types(self) -> list[tuple[str, str]]:
        """List of supported submission types: List[(long name, short name)]"""
        return [("api", "a")]  # Website submission done manually
    
    @property
    def rate_limit(self) -> int:
        """How long to wait between each submission"""
        # 5 minutes between submissions to avoid rate limits
        return 300
    
    def submit(self, url: str, visibility_type: str, submission_type: str) -> str | Exception:
        super().submit(url, visibility_type, submission_type)
        
        if submission_type == "a":
            return self.submit_api(url)
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")
        

    def submit_api(self, url: str) -> str | Exception:
        api_key = os.getenv("JOESANDBOX_API_KEY", "NOKEY")
        if api_key == "NOKEY":
            return Exception("JOESANDBOX_API_KEY not set in environment variables")
        
        api_url = "https://www.joesandbox.com/api/v2/submission/new"
        
        files = {
            "apikey": (None, api_key),
            "url": (None, url),
            "internet-access": (None, "1"),
            "systems[]": (None, "w10x64"),
            "hybrid-code-analysis": (None, "1"),
            "email-notification": (None, "0"),
            "accept-tac": (None, "1"),
        }

        try:
            response = httpx.post(api_url, files=files, timeout=60.0)
            print(f"[DEBUG] Raw API response:\n{response.text}")

            if response.status_code != 200:
                logging.error(f"[{self.name[0]}] API request failed with status {response.status_code}: {response.text}")
                return Exception(f"API request failed with status {response.status_code}: {response.text}")

            res = response.json()
            submission_id = res.get("data", {}).get("submission_id")
            if submission_id:
                return f"https://www.joesandbox.com/analysis/{submission_id}/summary"
            else:
                logging.error(f"[{self.name[0]}] No submission_id in response: {res}")
                return Exception(f"No submission_id in response: {res}")

        except Exception as e:
            logging.exception(f"[{self.name[0]}] Unexpected error: {e}")
            return e
