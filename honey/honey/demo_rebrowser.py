from playwright.sync_api import Playwright, sync_playwright, expect

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"], channel="chrome")
    context = browser.new_context(no_viewport=True)
    page = context.new_page()
    page.add_init_script("""
    delete window.__playwright__binding__;
    delete window.__pwInitScripts;
    """)
    page.goto("https://bot-detector.rebrowser.net/")
    # Currently passes all tests
    print(page.url)
    input("Wait")
    page.goto("https://dash.cloudflare.com/login")

    input("Wait")

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
