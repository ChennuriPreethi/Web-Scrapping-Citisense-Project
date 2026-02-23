# Web Scraping – Citisense Project (Traffic + Weather + Events)

A Python + Streamlit project that scrapes and processes public data sources to support **smart-city style analytics**:
- **UK Road Traffic (DfT Manual Count Points + AADF)** scraping and preprocessing
- **Weather forecasts** scraping + analytics
- **London events** scraping (to study possible traffic impact)
- A multi-page **Streamlit dashboard** to explore the datasets

---

## What’s inside this repo

### 1) Streamlit Dashboard
Main entrypoint: `home.py`

It provides a sidebar navigation with:
- **Home**
- **Traffic**
- **Weather**
- **Events** 
- Pages:
- `traffic_page.py` → reads traffic table from DB and shows filters + charts (region, authority, road, year range, etc.) 
- `weather_page.py` → reads weather table from DB and shows KPIs + charts (rainy/sunny/cloudy intervals, hottest day, max wind, etc.)
- `events_page.py` → scrapes event listings, stores events, and runs time-based analysis 

---

### 2) Traffic scraping + data outputs (DfT)
This repo includes scrapers to extract **Manual Count Points** data from the UK DfT road traffic statistics site:

- `LA_CountPoints.py`  
  Visits the DfT Local Authorities list and downloads the **Count points JSON** for each authority into `countpoints_json/`

- `MA_CountPoints.py`  
  Reads those JSON files to collect `count_point_id` values, then visits each manual count point page and scrapes:
  - **Site details** (region, authority, road name, junctions, coordinates, etc.)
  - **AADF table** (vehicle counts by year)  
  Output:
  - `site_details.csv`
  - `aadf_details.csv`

---

### 3) Traffic preprocessing + database load
You have two scripts for preprocessing and loading traffic datasets:

- `Traffic_Data_Preprocessing.py`  
  Loads `site_details.csv` and `aadf_details.csv`, performs cleaning + feature engineering:
  - forward-fill missing AADF values
  - remove duplicates
  - YoY growth columns per vehicle type
  - total traffic + YoY growth
  - outlier z-scores + scaling
  Then uploads into a DB table: `traffic_data` 

- `load_to_sqlserver.py`  
  Similar preprocessing logic, plus SQL Server helpers:
  - ensures DB exists
  - maps pandas dtypes → SQL Server types
  - intended to create/load a `traffic_data` table into SQL Server

---

### 4) Events scraping + storage
- `events_page.py` / `scrapper.py`  
  Scrapes event listings using Selenium + BeautifulSoup, parses event text, derives:
  - title
  - start/end datetime
  - venue (when available)
  - month/day/hour breakdown
  Stores in a DB table called `london_events` 

Notes:
- Uses spaCy model `en_core_web_sm` (already pinned in requirements). 

---

### 5) Weather scraping + storage
- `weather_data.py`  
  Uses Selenium and writes forecasts into a DB table (the dashboard expects: `weather_forecasts`). 

---

## Tech stack
- Python
- Streamlit
- Selenium + BeautifulSoup
- pandas / numpy / scipy
- SQLAlchemy (+ pyodbc for SQL Server)
- Altair + amCharts (embedded in Streamlit)
- spaCy `en_core_web_sm` 

---

## Setup

### 1) Clone
```bash
git clone https://github.com/ChennuriPreethi/Web-Scrapping-Citisense-Project.git
cd Web-Scrapping-Citisense-Project
```

## Create a virtual environment
python -m venv .venv
#### Windows:
.venv\Scripts\activate
#### macOS/Linux:
source .venv/bin/activate

## Install dependencies
pip install -r requirements.txt

## Run the Streamlit Dashboard
streamlit run home.py

## Typical workflow
### A. Traffic (DfT)
#### 1. Download JSON per authority:
python LA_CountPoints.py

#### 2. Scrape manual count point pages to create CSV outputs:
python MA_CountPoints.py

#### 3. Preprocess + upload traffic table:
python Traffic_Data_Preprocessing.py

### B. Weather
#### 1. Run scraper
python weather_data.py

#### 2. Open dashboard → Weather page (reads weather_forecasts)

### C. Events
- Open dashboard → Events page
- Scrapes events via Selenium
- Stores in london_events
- Displays time-based insights
