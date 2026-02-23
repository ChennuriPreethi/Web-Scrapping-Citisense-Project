import os
import re
import json
from typing import Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


LISTING_URL = "https://roadtraffic.dft.gov.uk/local-authorities"

# --- Driver factory (env-configurable) --------------------------------

def build_chrome(headless: Optional[bool] = None):
    # headless mode
    if headless is None:
        headless_env = os.getenv("CHROME_HEADLESS", os.getenv("HEADLESS", "1")).lower()
        headless = headless_env in ("1", "true", "yes", "y")

    opts = Options()
    if headless:
        # modern headless
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    # Custom Chrome binary via env
    chrome_binary = os.getenv("CHROME_BINARY")
    if chrome_binary and os.path.exists(chrome_binary):
        opts.binary_location = chrome_binary

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    wait = WebDriverWait(driver, 15)
    return driver, wait

def wait_css(wait: WebDriverWait, selector: str):
    return wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))

def click_when_clickable(wait: WebDriverWait, locator):
    el = wait.until(EC.element_to_be_clickable(locator))
    el.click()
    return el

# --- Scrape logic -----------------------------------------------------

def scrape_all_authorities():
    driver, wait = build_chrome()
    results = []
    try:
        # Load listing and get row count
        driver.get(LISTING_URL)
        table = wait_css(wait, "#table-sparkline")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        total = len(rows)
        print(f"→ Will click through {total} authority links…\n")

        for i in range(total):
            # Re-open listing fresh each loop to avoid stale elements
            driver.get(LISTING_URL)
            table = wait_css(wait, "#table-sparkline")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

            # if the table changes mid-loop
            if i >= len(rows):
                print(f"Row {i} no longer available, skipping.")
                continue

            row = rows[i]
            link = row.find_element(By.TAG_NAME, "a")
            name = link.text.strip()

            # Click the authority link
            click_when_clickable(wait, (By.LINK_TEXT, name))

            # Detail page header + URL
            try:
                header = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                ).text
            except Exception:
                header = "(No H1 found)"

            detail_url = driver.current_url

            print(f"{i+1}/{total} clicked → {name}")
            print(f"        header: {header}")
            print(f"        URL: {detail_url}")

            results.append({"name": name, "header": header, "url": detail_url})

            # Wait for the metrics table to appear
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )

            # Find the JSON link in the row whose first header cell is “Count points”
            json_link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//table//tr[th[normalize-space(.)='Count points']]//a[normalize-space(.)='JSON']"
                ))
            )

            json_url = json_link.get_attribute("href")
            print("        JSON URL:", json_url)

            # Open JSON URL and read body text
            driver.get(json_url)
            body = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            raw = body.text
            data = json.loads(raw)

            # Save JSON
            safe = re.sub(r"\W+", "_", header) or "authority"
            os.makedirs("countpoints_json", exist_ok=True)
            out_path = f"countpoints_json/{safe}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"        saved JSON to {out_path}\n")

            # Go back to the authority page
            driver.get(detail_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )

        print("\nDone clicking all authority links.")
        return results

    finally:
        driver.quit()


if __name__ == "__main__":
    scrape_all_authorities()
