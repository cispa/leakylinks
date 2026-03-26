from patchright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    context = playwright.chromium.launch_persistent_context(user_data_dir="data_dirs/joe", channel="chrome", headless=False, no_viewport=True)
    page = context.new_page()
    page.goto("https://www.joesandbox.com/")
    # page.pause()
    input("Please login to JoeSandbox, then press enter")
    # ---------------------
    context.close()


with sync_playwright() as playwright:
    run(playwright) 