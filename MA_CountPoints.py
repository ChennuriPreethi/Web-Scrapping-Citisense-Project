import os
import json
import time
import subprocess
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ——————— Config ———————

BASE_JSON_DIR = "countpoints_json"
BASE_URL      = "https://roadtraffic.dft.gov.uk/manualcountpoints/{}"

HEADLESS = os.getenv("HEADLESS", "0") == "1"
CHROME_BINARY = os.getenv("CHROME_BINARY")

# ——————— Driver helpers ———————

def build_driver():
    opts = Options()
    if os.getenv("CHROME_HEADLESS", os.getenv("HEADLESS","1")) in ("1","true","yes","y"):
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1440,900")
    bin_path = os.getenv("CHROME_BINARY")
    if bin_path and os.path.exists(bin_path):
        opts.binary_location = bin_path
    service = Service(ChromeDriverManager().install())
    d = webdriver.Chrome(service=service, options=opts)
    d.set_page_load_timeout(30)
    return d

driver = build_driver()
wait   = WebDriverWait(driver, 12)

# ——————— Setup Selenium & BeautifulSoup ———————

def safe_get(url: str, retries: int = 2):
    """Wrap driver.get; if session dies, rebuild driver and retry."""
    global driver, wait
    last_err = None
    for _ in range(retries + 1):
        try:
            driver.get(url)
            return
        except (InvalidSessionIdException, WebDriverException) as e:
            last_err = e
            try:
                driver.quit()
            except Exception:
                pass
            driver = build_driver()
            wait = WebDriverWait(driver, 10)
            time.sleep(0.3)
    raise last_err

def get_field(soup, label):
    """Grab a table row by its <th> text and return the <td> (kept from your original)."""
    for row in soup.select("table tr"):
        th = row.find("th")
        if th and th.get_text(strip=True) == label:
            td = row.find("td")
            return td.get_text(strip=True) if td else ""
    return ""

# ——————— Load all count_point_ids ———————

all_ids = set()
for fn in os.listdir(BASE_JSON_DIR):
    if fn.endswith(".json"):
        data = json.load(open(os.path.join(BASE_JSON_DIR, fn), encoding="utf-8"))
        all_ids |= {entry["count_point_id"] for entry in data.get("data", [])}
all_ids = sorted(all_ids)[:2000]   # For first 2000 for testing purposes    

# ——————— Scrape each page via Selenium ———————

site_records = []
aadf_records = []

for cp_id in all_ids:
    url = BASE_URL.format(cp_id)
    safe_get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.govuk-table"))
        )
    except TimeoutException:
        print(f"No tables appeared for {cp_id}, skipping")
        continue

    soup = BeautifulSoup(driver.page_source, "html.parser")
    # --- pull out all the govuk tables ---
    tables = soup.find_all("table", class_="govuk-table")
    if len(tables) < 2:
        raise RuntimeError(f"Expected ≥2 tables, got {len(tables)}")
    
    # SITE DETAILS (the first govuk-table)
    site_details = {}
    for tr in tables[0].select("tbody tr"):
        label = tr.select("td")[0].get_text(strip=True)
        value = tr.select("td")[1].get_text(strip=True)
        site_details[label] = value

    if "Link length" in site_details:
        txt = site_details.pop("Link length")
        km    = pd.Series([txt]).str.extract(r"([\d\.]+)\s*km")[0].iloc[0]
        miles = pd.Series([txt]).str.extract(r"([\d\.]+)\s*miles")[0].iloc[0]
        site_details["Link_km"]    = km
        site_details["Link_miles"] = miles

    if "Easting, northing" in site_details:
        e, n = [x.strip() for x in site_details.pop("Easting, northing").split(",")]
        site_details["Easting"]  = e
        site_details["Northing"] = n

    if "Latitude, longitude" in site_details:
        lat, lon = [x.strip() for x in site_details.pop("Latitude, longitude").split(",")]
        site_details["Latitude"]  = lat
        site_details["Longitude"] = lon

    site_details["count_point_id"] = cp_id
    site_records.append(site_details)

    # Locate the AADF table by finding the <th> whose text is “Year”
    year_th = soup.find("th", string="Year")
    if not year_th:
        raise RuntimeError("Couldn't find the AADF table header ‘Year’")
    aadf_table = year_th.find_parent("table")
    # Grab headers from its <thead>
    aadf_headers = [th.get_text(strip=True) for th in aadf_table.select("thead th")]
    # Grab each row from its <tbody>
    for tr in aadf_table.select("tbody tr"):
        vals = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not vals:
            continue
        row = dict(zip(aadf_headers, vals))
        row["count_point_id"] = cp_id
        aadf_records.append(row)
    print(f"Scraped {cp_id}")
driver.quit()

# turn into pandas & save
df_site = pd.DataFrame(site_records)
df_aadf = pd.DataFrame(aadf_records)
df_combined = pd.merge(df_aadf,df_site,on="count_point_id",how="left",suffixes=("_AADF","_SITE"))

df_site.rename(columns={
    "Region":             "region",
    "Local authority":    "local_authority",
    "Road name":          "road_name",
    "Road classification": "road_classification",
    "Managed by":         "managed_by",
    "Road type":          "road_type",
    "Start junction":     "start_junction",
    "End junction":       "end_junction",
    "Link_km":            "link_km",
    "Link_miles":         "link_miles",
    "Easting":            "easting",
    "Northing":           "northing",
    "Latitude":           "latitude",
    "Longitude":          "longitude",
    "count_point_id":     "count_point_id"
}, inplace=True)

df_aadf.rename(columns={
    "Year":                           "year",
    "Count method":                   "count_method",
    "Pedal cycles":                   "pedal_cycles",
    "Two wheeled motor vehicles":     "two_wheeled_motor_vehicles",
    "Cars and taxis":                 "cars_and_taxis",
    "Buses and coaches":              "buses_and_coaches",
    "Light goods vehicles":           "light_goods_vehicles",
    "Heavy goods vehicles":           "heavy_goods_vehicles",
    "All motor vehicles":             "all_motor_vehicles",
    "count_point_id":                 "count_point_id",
}, inplace=True)

df_site.to_csv(f"site_details.csv", index=False)
df_aadf.to_csv(f"aadf_details.csv", index=False)


