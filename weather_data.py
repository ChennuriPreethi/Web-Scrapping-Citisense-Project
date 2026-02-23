from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import date
import numpy as np
import re
from sqlalchemy import create_engine, MetaData, Table, text, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
import os
# ─── CONFIG ──────────────────────────────────────────────────────

def get_database_url() -> str:
    try:
        import streamlit as st
        if "db2" in st.secrets and "uri" in st.secrets["db2"]:
            return st.secrets["db2"]["uri"]
    except Exception:
        pass

    # Standard environment variable
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    raise RuntimeError(
        "No database URL found. Set env var DATABASE_URL "
        "add it to st.secrets['db2']['uri']."
)

DATABASE_URL = get_database_url()

BASE     = "https://weather.metoffice.gov.uk"
LIST_URL = BASE + "/forecast/regional/se/list"

opts = Options()
opts.headless = True
driver = webdriver.Chrome(options=opts)
driver.get(LIST_URL)

# ─── SCRAPER HELPERS ────────────────────────────────────────────────────

def parse_day_tab(day_tab):
    out = {}
    tl = day_tab.find_element(By.CSS_SELECTOR, "h3.tab-day time")
    out["date"]      = tl.get_attribute("datetime")
    out["day_label"] = tl.text.strip()
    out["max_temp"]  = day_tab.find_element(
        By.CSS_SELECTOR, ".tab-temp-high span[data-unit='temperature']"
    ).text.strip()
    out["min_temp"]  = day_tab.find_element(
        By.CSS_SELECTOR, ".tab-temp-low span[data-unit='temperature']"
    ).text.strip()
    sr = day_tab.find_element(By.CSS_SELECTOR, ".sunrise-sunset.sunrise time")
    ss = day_tab.find_element(By.CSS_SELECTOR, ".sunrise-sunset.sunset time")
    out["sunrise_datetime"] = sr.get_attribute("datetime")
    out["sunrise_time"]     = sr.text.strip()
    out["sunset_datetime"]  = ss.get_attribute("datetime")
    out["sunset_time"]      = ss.text.strip()
    out["summary"] = day_tab.find_element(
        By.CSS_SELECTOR, ".summary-text span"
    ).text.strip()
    return out

def parse_forecast_table(driver):
    tbl = driver.find_element(By.CSS_SELECTOR, "table.forecast-table")
    times = [el.text.strip() for el in tbl.find_elements(
        By.CSS_SELECTOR, "thead .time-step-hours"
    )]
    keep = {"Weather symbols","Temperature","Wind direction and speed"}
    rows = {}
    for row in tbl.find_elements(By.CSS_SELECTOR, "tbody tr"):
        raw = row.find_element(By.TAG_NAME, "th").text.strip()
        name = raw.splitlines()[0].strip()
        if name not in keep:
            continue
        vals = []
        for td in row.find_elements(By.TAG_NAME, "td"):
            img = td.find_elements(By.CSS_SELECTOR, "img.weather-symbol-icon")
            if img:
                vals.append(img[0].get_attribute("title"))
            else:
                # data-value, data-c, data-mph, data-kph or text
                div = None
                for attr in ("data-value","data-c","data-mph","data-kph"):
                    found = td.find_elements(By.CSS_SELECTOR, f"div[{attr}]")
                    if found:
                        div = found[0]; break
                vals.append(div.text.strip() if div else td.text.strip())
        rows[name] = vals
    return {"times": times, "rows": rows}

# ─── SCRAPE ALL BOROUGHS ──────────────────────────────────────────────

# Greater London borough links
links = driver.find_elements(By.CSS_SELECTOR, "a[href^='/forecast/']")
boroughs = [a for a in links if a.text.strip().endswith(" (Greater London)")]

data = []
for a in boroughs:
    name = a.text.removesuffix(" (Greater London)").strip()
    driver.get(a.get_attribute("href"))
    WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,".day-tab-display")))
    today = parse_day_tab(driver.find_element(By.CSS_SELECTOR,".day-tab-display"))
    WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,"table.forecast-table")))
    hourly = parse_forecast_table(driver)
    data.append({"borough":name, "today":today, "hourly":hourly})
    driver.back()
    WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR,"a[href^='/forecast/']")))

driver.quit()

# ─── FLATTEN INTO PANDA DATAFRAMES ───────────────────────────────────

df = pd.DataFrame(data)
tod = df["today"].apply(pd.Series)
times = pd.DataFrame(df["hourly"].apply(lambda h:h["times"]).tolist(), index=df.index).add_prefix("time_")
rows = pd.DataFrame(df["hourly"].apply(lambda h:h["rows"]).tolist(), index=df.index)
temps = pd.DataFrame(rows.get("Temperature", []).tolist(), index=df.index).add_prefix("temp_")
winds = pd.DataFrame(rows.get("Wind direction and speed", []).tolist(), index=df.index).add_prefix("wind_")
df_flat = pd.concat([df.drop(["today","hourly"],axis=1), tod, times, temps, winds], axis=1)

# ─── PREPROCESSING & SCAFFOLDING ───────────────────────────────────────

df_flat["date"] = pd.to_datetime(df_flat["date"]).dt.date
for c in ("max_temp","min_temp"): 
    df_flat[c] = df_flat[c].str.replace(r"[^\d\.]","",regex=True).replace("", np.nan).astype(float)

# ensure all time/temp/wind columns exist
for prefix, fill, rng in [("time_",0,range(24)),("temp_",0.0,range(24)),("wind_",0.0,range(24))]:
    for h in rng:
        col = f"{prefix}{h}"
        if col not in df_flat:
            df_flat[col] = fill

# split wind into dir & speed
for c in [c for c in df_flat.columns if c.startswith("wind_") and not c.endswith(("_dir","_speed"))]:
    ext = df_flat[c].astype(str).str.extract(r"^([A-Z]+)\s+(\d+)")
    df_flat[c+"_dir"]   = ext[0].where(ext[0]!="", pd.NA)
    df_flat[c+"_speed"] = pd.to_numeric(ext[1], errors='coerce')
    df_flat.drop(columns=[c], inplace=True)

temp_cols = [c for c in df_flat.columns if re.fullmatch(r"temp_\d{1,2}", c)]
if temp_cols:
    df_flat[temp_cols] = df_flat[temp_cols].apply(
        lambda s: s.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
    )
    df_flat[temp_cols] = df_flat[temp_cols].replace("", np.nan).astype(float)

# parse times into real timestamps
for h in range(24):
    col = f"time_{h}"
    # pandas will coerce invalids to NaT
    df_flat[col] = pd.to_datetime(
        df_flat["date"].astype(str) + " " + df_flat[col].astype(str),
        format="%Y-%m-%d %H:%M",
        errors="coerce"
    )

# ─── DB APPEND-ONLY INSERT ────────────────────────────────────────────

engine    = create_engine(DATABASE_URL)
inspector = inspect(engine)

# If table missing: create it & add constraint
if not inspector.has_table('weather_forecasts'):
    df_flat.to_sql('weather_forecasts', engine, if_exists='append', index=False)
    with engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE weather_forecasts "
            "ADD CONSTRAINT uniq_borough_date UNIQUE (borough, date)"
        ))
else:
    # Table exists: drop old constraint, recreate, then upsert new rows
    metadata = MetaData()
    weather  = Table('weather_forecasts', metadata, autoload_with=engine)

    # build INSERT ... ON CONFLICT DO NOTHING
    stmt = pg_insert(weather).on_conflict_do_nothing(index_elements=['borough','date'])
    df_flat = df_flat.astype(object).where(pd.notnull(df_flat), None)
    records = df_flat.to_dict(orient='records')
    with engine.begin() as conn:
        conn.execute(stmt, records)

print("Weather data inserted.")

