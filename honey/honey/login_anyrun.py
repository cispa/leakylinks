from patchright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    context = playwright.chromium.launch_persistent_context(user_data_dir="data_dirs/anyrun", channel="chrome", headless=False, no_viewport=True)
    page = context.new_page()
    page.goto("https://app.any.run/")
    # page.pause()
    input("Please login to AnyRun, then press enter")
    # ---------------------
    context.close()


with sync_playwright() as playwright:
    run(playwright) 