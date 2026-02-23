import streamlit as st
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from sqlalchemy import create_engine, text
import pandas as pd
import re, spacy, time
import altair as alt

# ------------------------- STREAMLIT CONFIG -------------------------

st.set_page_config(page_title="Citisense Events Page", layout="wide", initial_sidebar_state="expanded")
alt.data_transformers.enable("default", max_rows=None)


# ------------------------- LOAD NLP -------------------------

@st.cache_resource
def load_nlp():
    return spacy.load("en_core_web_sm")

nlp = load_nlp()


# ------------------------- DB SETUP (SQL SERVER + DEDUPE + AUTO TABLE) -------------------------

@st.cache_resource
def init_engine():
    server = st.secrets["db1"]["server"]
    database = st.secrets["db1"]["database"]
    username = st.secrets["db1"]["username"]
    password = st.secrets["db1"]["password"]
    driver = st.secrets["db1"]["driver"]

    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes&Encrypt=no"
    )

    engine = create_engine(conn_str, fast_executemany=True)
    return engine

engine = init_engine()

def ensure_table_exists():
    """Creates the SQL Server table if not exists."""
    create_sql = """
    IF NOT EXISTS (
        SELECT * FROM sysobjects WHERE name='london_events' AND xtype='U'
    )
    CREATE TABLE london_events (
        id INT IDENTITY(1,1) PRIMARY KEY,
        title NVARCHAR(500),
        start DATETIME NULL,
        [end] DATETIME NULL,
        venue NVARCHAR(500),
        raw NVARCHAR(MAX),
        month_label NVARCHAR(20),
        dow NVARCHAR(20),
        hour INT NULL,
        duration_hours FLOAT NULL,
        year INT NULL,
        event_hash NVARCHAR(200) UNIQUE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def fetch_saved_events():
    """Loads existing events."""
    ensure_table_exists()
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM london_events"), conn)
    return df


def save_to_database(df: pd.DataFrame):
    """Insert only NEW events — dedupe using event_hash."""
    ensure_table_exists()

    if df.empty:
        return

    df = df.copy()
    df["event_hash"] = (
        df["title"].astype(str)
        + df["start"].astype(str)
        + df.get("venue", "").astype(str)
    ).apply(lambda x: str(abs(hash(x))))

    # existing hashes
    with engine.connect() as conn:
        existing = pd.read_sql(text("SELECT event_hash FROM london_events"), conn)

    existing_hashes = set(existing["event_hash"].astype(str).tolist())
    df_new = df[~df["event_hash"].isin(existing_hashes)].copy()

    if df_new.empty:
        st.info("No new events found. Database is already up to date.")
        return

    with engine.begin() as conn:
        df_new.to_sql(
            "london_events",
            con=conn,
            index=False,
            if_exists="append"
        )

    st.success(f"Inserted {len(df_new)} new events (duplicates skipped).")


# ------------------------- SCRAPE PAGES -------------------------

def scrape_event_pages(base_url: str, total_pages: int) -> pd.DataFrame:
    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_opts)
    all_events = []

    try:
        for page in range(total_pages):
            url = f"{base_url}&page={page}"
            driver.get(url)
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            raw_text = soup.body.get_text(separator=" ", strip=True) if soup.body else ""
            df_page = parse_events(raw_text)

            if not df_page.empty:
                df_page = df_page[
                    ~df_page["Event Name"].str.contains(
                        r"(Your choice regarding cookies|Previous page|Next page|Useful links|Showing \d+ - \d+ of \d+ results)",
                        flags=re.IGNORECASE,
                        na=False,
                    )
                ]

            all_events.append(df_page)

    finally:
        driver.quit()

    if not all_events:
        return pd.DataFrame(columns=["Event Name"])

    out = pd.concat(all_events, ignore_index=True)
    out = out.drop_duplicates(subset=["Event Name"])
    return out


# ------------------------- EVENT PARSING -------------------------

def parse_events(raw_text: str) -> pd.DataFrame:
    event_pattern = re.compile(
        r"(?P<name>.+?)\s+Page type:\s*Event\s+Date\(s\):\s*(?P<dates>.+?)"
        r"(?=(?:\s+[A-Z].+?Page type:\s*Event)|\Z)",
        re.DOTALL
    )
    events = []
    for match in event_pattern.finditer(raw_text):
        name = match.group('name').split("Page type:")[0].strip()
        if name:
            events.append({"Event Name": name})
    return pd.DataFrame(events)


# ------------------------- ANALYSIS HELPERS -------------------------

def _coalesce_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _extract_venue(s: str):
    if not isinstance(s, str):
        return None
    m = re.search(r"Venue:\s*([A-Za-z0-9 ,’'&()./\-]+)", s)
    return m.group(1).strip() if m else None


def _extract_title(s: str):
    if not isinstance(s, str):
        return None
    parts = [p.strip() for p in s.split(".") if p.strip()]
    if parts:
        tail = parts[-1]
        if len(tail) < 3 or "venue" in tail.lower():
            words = re.split(r"\s+", s.strip())
            return " ".join(words[-6:]).strip()
        return tail
    words = re.split(r"\s+", s.strip())
    return " ".join(words[-6:]).strip()


def _parse_start_end(s: str):
    if not isinstance(s, str):
        return pd.NaT, pd.NaT
    left = s.split("Venue:")[0].strip()
    if "-" in left:
        a, b = left.split("-", 1)
        a = a.strip().strip(",")
        b = b.strip().strip(",")
    else:
        a, b = left, None

    start_dt = pd.to_datetime(a, errors="coerce")
    end_dt = None

    if b:
        # If end missing a date, borrow date from start
        if re.search(r"[A-Za-z]{3,}\s+\d{1,2}\s+\d{4}", b) is None and pd.notna(start_dt):
            b2 = f"{start_dt.strftime('%b %d %Y')} {b}"
            end_dt = pd.to_datetime(b2, errors="coerce")
        else:
            end_dt = pd.to_datetime(b, errors="coerce")

    if end_dt is None and pd.notna(start_dt):
        end_dt = start_dt

    return start_dt, end_dt


def _make_unique_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure unique column names (Narwhals requirement for Altair)."""
    if df.columns.is_unique:
        return df
    counts = {}
    new_cols = []
    for c in df.columns:
        if c not in counts:
            counts[c] = 0
            new_cols.append(c)
        else:
            counts[c] += 1
            new_cols.append(f"{c}_{counts[c]}")
    df = df.copy()
    df.columns = new_cols
    return df


@st.cache_data(show_spinner=False)
def parse_from_single_text_column(df_text: pd.DataFrame, text_col: str) -> pd.DataFrame:
    recs = []
    for s in df_text[text_col].astype(str):
        st_dt, en_dt = _parse_start_end(s)
        recs.append({
            "title": _extract_title(s),
            "start": st_dt,
            "end": en_dt,
            "venue": _extract_venue(s),
            "raw": s
        })
    out = pd.DataFrame(recs)
    out["start"] = pd.to_datetime(out["start"], errors="coerce")
    out["end"]   = pd.to_datetime(out["end"],   errors="coerce").fillna(out["start"])
    out["month_start"] = out["start"].dt.to_period("M").dt.to_timestamp()
    out["month_label"] = out["start"].dt.to_period("M").astype(str)
    out["dow"]         = out["start"].dt.day_name()
    out["hour"]        = out["start"].dt.hour
    out["duration_hours"] = (out["end"] - out["start"]).dt.total_seconds() / 3600.0
    out["year"]        = out["start"].dt.year
    return _make_unique_cols(out)


def maybe_parse(df0: pd.DataFrame) -> pd.DataFrame:
    """If only 'Event Name' exists, parse from text. Otherwise, normalize."""
    if df0.empty:
        return df0

    name_col  = _coalesce_col(df0, ["Event Name", "event_name", "name", "title"])
    start_col = _coalesce_col(df0, ["start", "Start Date", "start_date", "Date", "date"])
    end_col   = _coalesce_col(df0, ["end", "End Date", "end_date"])
    venue_col = _coalesce_col(df0, ["venue", "Venue", "Location", "location"])

    # If we only have a single text column with event names/descriptions
    if start_col is None and name_col and df0.shape[1] == 1:
        return parse_from_single_text_column(df0, name_col)

    df = df0.copy()

    if start_col:
        df["start"] = pd.to_datetime(df[start_col], errors="coerce")
    if end_col:
        df["end"] = pd.to_datetime(df[end_col], errors="coerce")
    if "start" not in df:
        df["start"] = pd.NaT
    if "end" not in df:
        df["end"] = df["start"]

    if venue_col and "venue" not in df:
        df["venue"] = df[venue_col]
    if "title" not in df and name_col:
        df["title"] = df[name_col]

    df["month_start"]  = df["start"].dt.to_period("M").dt.to_timestamp()
    df["month_label"]  = df["start"].dt.to_period("M").astype(str)
    df["dow"]          = df["start"].dt.day_name()
    df["hour"]         = df["start"].dt.hour
    df["duration_hours"] = (df["end"] - df["start"]).dt.total_seconds() / 3600.0
    df["year"]         = df["start"].dt.year

    return _make_unique_cols(df)


# ------------------------- STREAMLIT UI -------------------------

def render_page():
    st.markdown("""
        <style>
        /* Import Google Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:ital,wght@0,100..900;1,100..900&display=swap');

        /* Apply font globally */
        html, body, [class*="st-"], h1, h2, h3, h4, h5, h6, p, div, span, label, .css-10trblm, .css-1v0mbdj {
            font-family: 'Montserrat', sans-serif !important;
        }

        /* Hide Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}

        /* Custom CSS Variables */
        :root {
            --primary-color: #1e3a8a;
            --secondary-color: #3b82f6;
            --accent-color: #10b981;
            --warning-color: #f59e0b;
            --danger-color: #ef4444;
            --dark-bg: #0f172a;
            --light-bg: #f8fafc;
            --card-bg: #ffffff;
            --text-primary: #1e293b;
            --text-secondary: #64748b;
            --border-color: #e2e8f0;
            --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            --gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }

        .block-container {
            padding-top: 0rem !important;
        }

        .main {
            padding-top: 0rem !important;
        }

        /* Button styling */
        .stButton>button {
            background-color: #1e3a8a;
            color: #ffffff;
            border: none;
            border-radius: 0.5rem;
            padding: 0.6rem 1.2rem;
            font-size: 1.1rem;
            font-weight: 600;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: background-color 0.2s ease;
        }
        .stButton>button:hover {
            background-color: #3b82f6;
            color: black;
        }

        .kpi-card {
            background: var(--card-bg);
            padding: 1.5rem;
            border-radius: 12px;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08);
            text-align: center;
            transition: transform 0.2s;
        }

        .kpi-card:hover {
            transform: translateY(-6px);
            background-color: #e0ebf5;
        }

        .kpi-title {
            font-size: 0.9rem;
            color: #6c757d;
            margin-bottom: 0.5rem;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
        }

        .kpi-value {
            font-size: 1.6rem;
            font-weight: bold;
            color: #2c3e50;
        }

        .kpi-icon {
            font-size: 1.1rem;
        }

        .green { color: #28a745; }
        .red { color: #dc3545; }
        .gray { color: #6c757d; }

        div[data-testid="stDataFrame"] {
            border-radius: 10px;
            box-shadow: var(--shadow);
        }

        /* Section Headers */
        .section-header {
            font-size: 1.5rem;
            font-weight: 600;
            color: var(--text-primary);
            margin: 2rem 0 1rem 0;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid var(--secondary-color);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        /* Data Tables */
        .dataframe {
            border-radius: 8px;
            overflow: hidden;
            box-shadow: var(--shadow);
        }

        /* Footer */
        .footer {
            margin-top: 4rem;
            padding: 2rem 0;
            text-align: center;
            border-top: 1px solid var(--border-color);
            color: var(--text-secondary);
            font-size: 0.9rem;
        }

        /* Responsive Design */
        @media (max-width: 768px) {
            .main-header {
                font-size: 2rem;
            }
            .metric-value {
                font-size: 2rem;
            }
            .section-header {
                font-size: 1.2rem;
            }
        }
        """, unsafe_allow_html=True)

    st.title("📅 London Events")

    # Session state for current events
    if "events_df" not in st.session_state:
        st.session_state["events_df"] = pd.DataFrame()

    base_url = st.text_input(
        "Enter URL",
        value="https://www.london.gov.uk/take-part/past-events"
    )

    tab_src, tab_ana = st.tabs(["Scrape Data", "Event Analysis"])

    # ------------------------- SCRAPE TAB -------------------------
    with tab_src:
        if st.button("Scrape"):
            with st.spinner("Scraping and parsing events..."):
                try:
                    # Probe first page to estimate total results → total pages
                    chrome_opts = Options()
                    chrome_opts.add_argument("--headless=new")
                    driver = webdriver.Chrome(options=chrome_opts)

                    try:
                        driver.get(f"{base_url}&page=0")
                        soup = BeautifulSoup(driver.page_source, "html.parser")
                        text_all = soup.get_text(" ", strip=True)

                        # Example text: "Showing 1 - 20 of 1157 results"
                        m = re.search(r"Showing\s+\d+\s*-\s*\d+\s+of\s+(\d+)\s+results", text_all)
                        total_results = int(m.group(1)) if m else 1200
                        per_page = 20
                        total_pages = (total_results // per_page) + (1 if total_results % per_page else 0)
                    finally:
                        driver.quit()

                    df_raw = scrape_event_pages(base_url, total_pages)

                    if not df_raw.empty:
                        df_parsed = maybe_parse(df_raw)
                        st.session_state["events_df"] = df_parsed.copy()

                        st.success(f"Scraped {len(df_parsed):,} parsed events across London")

                        # Save to database (deduped)
                        try:
                            save_to_database(df_parsed)
                        except Exception as se:
                            st.warning(f"Save skipped (DB issue): {se}")

                        st.dataframe(df_parsed.head(50), use_container_width=True)
                    else:
                        st.info("No events found on the pages.")

                except Exception as e:
                    st.error(f"Error during scraping: {e}")

        if st.button("Show Saved Events"):
            with st.spinner("Loading saved events from database..."):
                try:
                    df_saved = fetch_saved_events()
                    if not df_saved.empty:
                        df_parsed = maybe_parse(df_saved)
                        st.session_state["events_df"] = df_parsed.copy()
                        st.success(f"Loaded {len(df_parsed):,} saved events from SQL Server")
                        st.dataframe(df_parsed, use_container_width=True)
                    else:
                        st.info("No events saved yet in the database.")
                except Exception as e:
                    if 'does not exist' in str(e).lower() or 'undefined_table' in str(e).lower():
                        st.warning("No london_events table found in the database.")
                    else:
                        st.error(f"Failed to load saved events: {e}")

    # ------------------------- ANALYSIS TAB -------------------------
    with tab_ana:
        df_current = st.session_state.get("events_df", pd.DataFrame()).copy()

        if df_current.empty:
            st.info("No data to analyze yet. Please scrape or load saved events from the first tab.")
            return

        df = df_current.copy()

        # ---------- Filters ----------
        today = pd.Timestamp.today().normalize()

        valid_start = pd.to_datetime(df["start"], errors="coerce").dropna()
        if valid_start.empty:
            st.error("No valid event dates found to filter on.")
            dff = df.copy()
        else:
            min_d = valid_start.min()
            max_d = valid_start.max()

            with st.sidebar:
                st.header("Filters")
                with st.form("filters_form"):
                    date_range = st.slider(
                        "Date range",
                        min_value=min_d.to_pydatetime(),
                        max_value=max_d.to_pydatetime(),
                        value=(min_d.to_pydatetime(), max_d.to_pydatetime()),
                        key="date_range_slider",
                    )
                    submitted = st.form_submit_button("Apply filters")

            if submitted or "applied_range" not in st.session_state:
                st.session_state["applied_range"] = date_range

            date_range = st.session_state["applied_range"]

            mask = (
                df["start"] >= pd.to_datetime(date_range[0])
            ) & (
                df["start"] <= pd.to_datetime(date_range[1])
            )
            dff = df.loc[mask].copy()

        dff = _make_unique_cols(dff)
        dff = dff.dropna(subset=["start"]).sort_values("start")

        # ---------- KPIs ----------
        st.markdown('<div class="section-header">📈 Key Performance Indicators</div>', unsafe_allow_html=True)

        total = len(df)
        filtered = len(dff)
        unique_venues = int(dff["venue"].nunique()) if "venue" in dff.columns and dff["venue"].notna().any() else 0
        upcoming_count = int(dff.loc[dff["start"] >= today].shape[0])
        med_dur = float(dff["duration_hours"].median()) if "duration_hours" in dff.columns and dff["duration_hours"].notna().any() else None

        kpi_cols = st.columns(4)

        kpi_cols[0].markdown(
            f"""
            <div class='kpi-card'>
                <div class='kpi-title'><span class='kpi-icon'>📊</span> Total events</div>
                <div class='kpi-value'>{total:,}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        kpi_cols[1].markdown(
            f"""
            <div class='kpi-card'>
                <div class='kpi-title'><span class='kpi-icon'>🎯</span> In filter</div>
                <div class='kpi-value'>{filtered:,}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        kpi_cols[2].markdown(
            f"""
            <div class='kpi-card'>
                <div class='kpi-title'><span class='kpi-icon'>📍</span> Unique venues</div>
                <div class='kpi-value'>{unique_venues if unique_venues else "—"}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        kpi_cols[3].markdown(
            f"""
            <div class='kpi-card'>
                <div class='kpi-title'><span class='kpi-icon'>⏱️</span> Median duration (hrs)</div>
                <div class='kpi-value'>{f"{med_dur:.1f}" if med_dur is not None else "—"}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.divider()

        # ---------- Filtered Data Table ----------
        st.markdown('<div class="section-header">Filtered data</div>', unsafe_allow_html=True)
        show_cols = [
            c for c in ["title", "start", "end", "venue", "duration_hours", "year", "month_label", "dow", "hour"]
            if c in dff.columns
        ]
        st.dataframe(dff[show_cols], use_container_width=True)

        # ---------- Trends & Distributions ----------
        st.markdown('<div class="section-header">Trends & Distributions</div>', unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3, gap="medium")

        # Events per Month
        with col1:
            if "month_start" in dff.columns and dff["month_start"].notna().any():
                monthly = (
                    dff.dropna(subset=["month_start"])
                    .groupby("month_start")
                    .size()
                    .reset_index(name="events")
                    .sort_values("month_start")
                )
                monthly = _make_unique_cols(monthly)
                chart_month = (
                    alt.Chart(monthly)
                    .mark_bar()
                    .encode(
                        x=alt.X("month_start:T", title="Month"),
                        y=alt.Y("events:Q", title="Events"),
                        tooltip=[alt.Tooltip("month_start:T", title="Month"), "events:Q"],
                    )
                    .properties(height=320, title="Events per Month")
                )
                st.altair_chart(chart_month, use_container_width=True)
            else:
                st.info("No month data available.")

        # Day-of-week
        with col2:
            if "dow" in dff.columns and dff["dow"].notna().any():
                order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                dow_counts = dff["dow"].value_counts().rename_axis("dow").reset_index(name="events")
                dow_counts["dow"] = pd.Categorical(dow_counts["dow"], categories=order, ordered=True)
                dow_counts = dow_counts.sort_values("dow")
                dow_counts = _make_unique_cols(dow_counts)

                chart_dow = (
                    alt.Chart(dow_counts)
                    .mark_bar()
                    .encode(
                        x=alt.X("dow:N", title="Day of week", sort=order),
                        y=alt.Y("events:Q", title="Events"),
                        tooltip=["dow:N", "events:Q"],
                    )
                    .properties(height=320, title="Events per Day of Week")
                )
                st.altair_chart(chart_dow, use_container_width=True)
            else:
                st.info("No day-of-week data.")

        # Hour-of-day
        with col3:
            if "hour" in dff.columns and dff["hour"].notna().any():
                hour_df = dff.dropna(subset=["hour"]).copy()
                hour_df = _make_unique_cols(hour_df)
                chart_hour = (
                    alt.Chart(hour_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("hour:Q", bin=alt.Bin(step=1), title="Hour of day"),
                        y=alt.Y("count():Q", title="Events"),
                        tooltip=[alt.Tooltip("count():Q", title="Events")],
                    )
                    .properties(height=320, title="Events per Hour of Day")
                )
                st.altair_chart(chart_hour, use_container_width=True)
            else:
                st.info("No time-of-day data available.")

        # Duration & Year
        col4, col5 = st.columns(2, gap="medium")

        with col4:
            if "duration_hours" in dff.columns and dff["duration_hours"].notna().any():
                dur_df = dff.loc[dff["duration_hours"].between(0, 72)].copy()  # clip tails
                dur_df = _make_unique_cols(dur_df)
                chart_dur = (
                    alt.Chart(dur_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("duration_hours:Q", bin=alt.Bin(step=1), title="Duration (hours)"),
                        y=alt.Y("count():Q", title="Events"),
                        tooltip=[alt.Tooltip("count():Q", title="Events")],
                    )
                    .properties(height=260, title="Distribution of Event Duration")
                )
                st.altair_chart(chart_dur, use_container_width=True)
            else:
                st.info("No duration data available.")

        with col5:
            if "year" in dff.columns and dff["year"].notna().any():
                year_counts = (
                    dff.dropna(subset=["year"])
                    .groupby("year")
                    .size()
                    .reset_index(name="events")
                )
                year_counts = _make_unique_cols(year_counts)
                chart_year = (
                    alt.Chart(year_counts)
                    .mark_bar()
                    .encode(
                        x=alt.X("year:O", title="Year", sort="ascending"),
                        y=alt.Y("events:Q", title="Events"),
                        tooltip=["year:O", "events:Q"],
                    )
                    .properties(height=260, title="Events per Year")
                )
                st.altair_chart(chart_year, use_container_width=True)
            else:
                st.info("No year info available.")

        # Venue centric
        st.markdown('<div class="section-header">Top 10 Venues</div>', unsafe_allow_html=True)
        K_LINES = 10

        if "venue" in dff.columns and dff["venue"].notna().any():
            vt = dff.dropna(subset=["venue", "month_start"]).copy()
            if not vt.empty:
                top_venues_l = vt["venue"].value_counts().head(K_LINES).index.tolist()
                vtl = vt[vt["venue"].isin(top_venues_l)]
                trend = (
                    vtl.groupby(["venue", "month_start"])
                    .size()
                    .reset_index(name="events")
                    .sort_values("month_start")
                )
                trend = _make_unique_cols(trend)
                chart_trend = (
                    alt.Chart(trend)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("month_start:T", title="Month"),
                        y=alt.Y("events:Q", title="Events"),
                        color=alt.Color("venue:N", title="Venue"),
                        tooltip=[
                            "venue:N",
                            alt.Tooltip("month_start:T", title="Month"),
                            alt.Tooltip("events:Q", title="Events"),
                        ],
                    )
                    .properties(height=260, title="Top Venues – Events Over Time")
                )
                st.altair_chart(chart_trend, use_container_width=True)
            else:
                st.info("No month/venue combination data available.")
        else:
            st.info("No venue data available in this dataset.")


if __name__ == "__main__":
    render_page()
