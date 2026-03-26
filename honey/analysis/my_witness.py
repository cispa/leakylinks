import os
import time
import asyncio
from playwright.async_api import async_playwright

WITH_HAR = True # False

def create_timestamped_directory():
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    directory_name = f"screenshots_{timestamp}"
    os.makedirs(directory_name, exist_ok=True)
    return directory_name

async def take_screenshot(browser, url, output_path):
    try:
        page = await browser.new_page()
        if WITH_HAR:
            har_file_name = os.path.basename(output_path).removesuffix(".png") + ".har"
            har_path = os.path.join(os.path.dirname(output_path) , "hars")
            await page.route_from_har(os.path.join(har_path, har_file_name), update=True)
        print(url)
        await page.goto(url, timeout=60000)
        await page.screenshot(path=output_path, full_page=True)
        await page.close()
    except Exception as e:
        print(f"Failed to take screenshot of {url}: {e}")

def chunks(xs, n):
    n = max(1, n)
    return (xs[i:i+n] for i in range(0, len(xs), n))

async def main(url_file_path):
    # Read URLs from the file
    try:
        with open(url_file_path, 'r') as file:
            urls = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"File not found: {url_file_path}")
        return

    # Create timestamped directory
    screenshot_directory = create_timestamped_directory()

    for urls_chunk in chunks(urls, 100):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--ignore-certificate-errors"])

            tasks = []
            for url in urls_chunk:
                if not url.startswith("http"):
                    url = "http://" + url
                filename = url.replace("http://", "").replace("https://", "").replace("/", "_")
                output_path = os.path.join(screenshot_directory, f"{filename}.png")
                tasks.append(take_screenshot(browser, url, output_path))

            await asyncio.gather(*tasks)

            await browser.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_url_file>")
    else:
        url_file_path = sys.argv[1]
        asyncio.run(main(url_file_path))

