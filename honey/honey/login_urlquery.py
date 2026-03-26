from patchright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    context = playwright.chromium.launch_persistent_context(user_data_dir="data_dirs/urlquery", channel="chrome", headless=False, no_viewport=True)
    page = context.new_page()
    page.goto("https://urlquery.net/user/login")
    # page.goto(f"https://urlquery.net")
    # page.pause()
    input("Please login, then press enter")
    # ---------------------
    context.close()


with sync_playwright() as playwright:
    run(playwright) 