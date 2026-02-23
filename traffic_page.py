import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
import streamlit.components.v1 as components
import altair as alt

# ------------------------- STREAMLIT CONFIG -------------------------

# Set page config
st.set_page_config(page_title="Citisense Traffic Analytics", layout="wide", initial_sidebar_state="expanded")

# ------------------------- DATABASE SETUP -------------------------

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
# ------------------------- DATA LOADING -------------------------

def fetch_data():
    query = "SELECT * FROM traffic_data;"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

# ------------------------- MAIN DASHBOARD -------------------------

def render_page():
    df = fetch_data()
    # Custom CSS
    st.markdown("""
        <style>
        /* Import Google Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:ital,wght@0,100..900;1,100..900&display=swap');

        /* Apply font globally */
        html, body, [class*="st-"], h1, h2, h3, h4, h5, h6, p, div, span, label, .css-10trblm, .css-1v0mbdj
        {
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
        
        h1 {
            text-align: center !important;
            margin-top: 2rem;
            color: #1e3a8a;
            font-weight: 700;
        }
                
        .kpi-card {
            background: var(--card-bg);
            padding: 1.5rem;
            border-radius: 12px;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08);
            text-align: center;
            font-family: 'Segoe UI', sans-serif;
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
            
    </style> 
    """, unsafe_allow_html=True)

    # ------------------------- SIDEBAR -------------------------

    # Sidebar
    st.sidebar.markdown('### 📊 Data Management')

    # Search bar
    search_query = st.sidebar.text_input("Search", placeholder="Search site, road, region, authority...").lower().strip()

    if search_query != "":
        df = df[
            df.apply(
                lambda row: any(
                    str(row[col]).lower() == search_query
                    for col in ["road_name", "local_authority", "region", "start_junction", "end_junction"]
                    if col in df.columns
                ),
                axis=1,
            )
        ]
    
    # Region dropdown
    if 'region' in df.columns:
        regions = ['All'] + sorted(df['region'].dropna().unique().tolist())
        selected_region = st.sidebar.selectbox("Select Region", regions)
        if selected_region != 'All':
            df = df[df['region'] == selected_region]

    # Local Authority dropdown
    if 'local_authority' in df.columns:
        las = ['All'] + sorted(df['local_authority'].dropna().unique().tolist())
        selected_la = st.sidebar.selectbox("Select Local Authority", las)
        if selected_la != 'All':
            df = df[df['local_authority'] == selected_la]

    # Road Name dropdown
    if 'road_name' in df.columns:
        roads = ['All'] + sorted(df['road_name'].dropna().unique().tolist())
        selected_road = st.sidebar.selectbox("Select Road Name", roads)
        if selected_road != 'All':
            df = df[df['road_name'] == selected_road]

    # Year Range Filter
    
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    elif "date" in df.columns:
        df["year"] = pd.to_datetime(df["date"], errors="coerce").dt.year
    else:
        st.sidebar.error("No 'date' or 'year' column found.")
        st.stop()

    # Drop rows where year is missing
    df = df.dropna(subset=["year"])

    # If df becomes empty after filter/search, stop safely
    if df.empty:
        st.warning("No data matches your search/filters.")
        st.stop()

    # Convert back to int
    df["year"] = df["year"].astype(int)

    # Safe min/max year detection
    min_year = df['year'].min()
    max_year = df['year'].max()

    # If only one year exists
    if min_year == max_year:
        start_year = end_year = min_year
        st.sidebar.info(f"Only data for year {min_year} is available.")
    else:
        start_year, end_year = st.sidebar.slider(
            "Select year range",
            int(min_year),
            int(max_year),
            (int(min_year), int(max_year))
        )

    # Apply year filter
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)]


    # ------------------------- KEY PERFORMANCE INDICATOR -------------------------

    st.title("🚦 Traffic Analytics Dashboard")
    st.markdown('<div class="section-header">📈 Key Performance Indicators</div>', unsafe_allow_html=True)
    total_records = len(df)
    unique_roads = df['road_name'].nunique() if 'road_name' in df.columns else 0
    avg_flow = df['total_traffic'].mean() if 'total_traffic' in df.columns else 0
    peak_flow = df['total_traffic'].max() if 'total_traffic' in df.columns else 0

    yoy_growth = None
    if 'year' in df.columns and 'total_traffic' in df.columns:
        annual = df.groupby('year')['total_traffic'].sum().sort_index()
        if len(annual) >= 2:
            yoy_growth = (annual.iloc[-1] - annual.iloc[-2]) / annual.iloc[-2] * 100

    kpi_cols = st.columns(5)
    kpi_cols[0].markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>📄</span> Total Records</div><div class='kpi-value'>{total_records:,}</div></div>""", unsafe_allow_html=True)
    kpi_cols[1].markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🛣️</span> Unique Roads</div><div class='kpi-value'>{unique_roads:,}</div></div>""", unsafe_allow_html=True)
    kpi_cols[2].markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🔁</span> Average Flow</div><div class='kpi-value'>{avg_flow:,.0f}</div></div>""", unsafe_allow_html=True)
    kpi_cols[3].markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🚦</span> Peak Flow</div><div class='kpi-value'>{peak_flow:,.0f}</div></div>""", unsafe_allow_html=True)
    if yoy_growth is not None:
        sign = "+" if yoy_growth >= 0 else ""
        growth_val = f"<span class='green'>{sign}{yoy_growth:.1f}%</span>" if yoy_growth >= 0 else f"<span class='red'>{yoy_growth:.1f}%</span>"
    else:
        growth_val = "<span class='gray'>N/A</span>"
    kpi_cols[4].markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>📈</span> YoY Growth</div><div class='kpi-value'>{growth_val}</div></div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ------------------------- MAP -------------------------

    # Map of count points using lat & lon
    st.markdown('<div class="section-header">Map of Count Point Locations</div>', unsafe_allow_html=True)
    if 'latitude' in df.columns and 'longitude' in df.columns:
        st.map(df.rename(columns={'latitude':'lat','longitude':'lon'})[['lat','lon']])

    # ------------------------- DATA ANALYSIS -------------------------

    col1,col2 = st.columns(2, gap="medium")
    with col1:
        st.markdown('<div class="section-header">Traffic Distribution Analysis</div>', unsafe_allow_html=True)
        # Total Traffic Flow Over Years
        if 'year' in df.columns and 'total_traffic' in df.columns:
            annual_flow = df.groupby('year')['total_traffic'].sum().reset_index()
            data_line = annual_flow.to_dict(orient='records')
        
            chart_line = {
                "type": "serial",
                "theme": "light",
                "dataProvider": data_line,
                "valueAxes": [{"unit": "", "position": "left", "title": "Total Traffic Flow"}],
                "graphs": [{"balloonText": "[[value]]", "valueField": "total_traffic", "bullet": "round", "lineThickness": 2}],
                "categoryField": "year",
                "categoryAxis": {"gridPosition": "start", "labelRotation": 0, "title": "Year"}
            }
            html_line = f"""
            <div id="annual_chart" style="width:100%; height:350px;"></div>
            <script src="https://cdn.amcharts.com/lib/3/amcharts.js"></script>
            <script src="https://cdn.amcharts.com/lib/3/serial.js"></script>
            <script src="https://cdn.amcharts.com/lib/3/themes/light.js"></script>
            <script>AmCharts.makeChart("annual_chart", {json.dumps(chart_line)});</script>
            """
            components.html(html_line, height=450)

    with col2:
        st.markdown('<div class="section-header">Vehicle Type Distribution (Latest Year)</div>', unsafe_allow_html=True)
        # Vehicle Type Distribution 
        latest = df['year'].max()
        vehicle_latest = df[df['year'] == latest][["cars_and_taxis", "light_goods_vehicles", "pedal_cycles", "buses_and_coaches", "heavy_goods_vehicles"]].sum().reset_index()
        vehicle_latest.columns = ["type", "count"]
        pie_data = [{"type": r["type"], "count": int(r["count"])} for _, r in vehicle_latest.iterrows()]

        pie_chart = f"""
            <div id=\"chartdiv3\" style=\"height:400px;\"></div>
            <script src=\"https://cdn.amcharts.com/lib/5/index.js\"></script>
            <script src=\"https://cdn.amcharts.com/lib/5/percent.js\"></script>
            <script src=\"https://cdn.amcharts.com/lib/5/themes/Animated.js\"></script>
            <script>am5.ready(function() {{
                var root = am5.Root.new(\"chartdiv3\");
                root.setThemes([am5themes_Animated.new(root)]);
                var chart = root.container.children.push(am5percent.PieChart.new(root, {{ layout: root.verticalLayout }}));
                var series = chart.series.push(am5percent.PieSeries.new(root, {{name: \"Vehicle Types\",valueField: \"count\",categoryField: \"type\"}}));
                var data = {json.dumps(pie_data)};
                series.data.setAll(data);}});
            </script>
            """
        components.html(pie_chart, height=450)

    # Time Series Chart of Total Traffic
    st.markdown("<div class='section-header'>📈 Total Traffic Trends by Borough</div>", unsafe_allow_html=True)
    time_series = df.groupby(['year', 'local_authority'])['total_traffic'].sum().reset_index()
    chart = alt.Chart(time_series).mark_line(point=True).encode(
        x=alt.X('year:O', title='Year'),
        y=alt.Y('total_traffic:Q', title='Total Traffic'),
        color='local_authority:N',
        tooltip=['year', 'local_authority', 'total_traffic']
    ).properties(height=400)
    st.altair_chart(chart, use_container_width=True)

    col1, col2 = st.columns(2, gap="medium")
    with col1:
        # Top 10 Roads with Highest Total Traffic
        st.markdown("<div class='section-header'>🛣️ Top 10 Roads by Total Traffic</div>", unsafe_allow_html=True)
        top_roads = df.groupby("road_name")["total_traffic"].sum().nlargest(10).reset_index()
        st.bar_chart(top_roads.set_index("road_name"))

    with col2:
        # Region-wise Traffic Distribution
        st.markdown("<div class='section-header'>🌍 Region-wise Total Traffic Distribution</div>", unsafe_allow_html=True)
        region_dist = df.groupby("region")["total_traffic"].sum().reset_index()
        region_chart = alt.Chart(region_dist).mark_bar().encode(
            x=alt.X('region:N', sort='-y', title='Region'),
            y=alt.Y('total_traffic:Q', title='Total Traffic'),
            tooltip=['region', 'total_traffic']
        ).properties(height=460)
        st.altair_chart(region_chart, use_container_width=True)

    # Yearly Average Traffic per Vehicle Type
    if all(col in df.columns for col in ["cars_and_taxis", "buses_and_coaches", "light_goods_vehicles"]):
        st.markdown("<div class='section-header'>🚗 Average Yearly Traffic by Vehicle Type", unsafe_allow_html=True)
        vehicle_avg = df.groupby("year")[["cars_and_taxis", "buses_and_coaches", "light_goods_vehicles"]].mean().reset_index()
        vehicle_df = pd.melt(vehicle_avg, id_vars=["year"], var_name="vehicle_type", value_name="average_traffic")
        chart2 = alt.Chart(vehicle_df).mark_area(opacity=0.7).encode(
            x='year:O', y='average_traffic:Q', color='vehicle_type:N', tooltip=['year', 'vehicle_type', 'average_traffic']).properties(height=400)
        st.altair_chart(chart2, use_container_width=True)

    col1, col2 = st.columns(2, gap="medium")
    with col1:
    # Most Used Count Method
        if 'count_method' in df.columns:
            st.markdown("<div class='section-header'> 🧮 Most Used Count Method", unsafe_allow_html=True)
            method_counts = df['count_method'].value_counts().reset_index()
            method_counts.columns = ["method", "count"]
            st.bar_chart(method_counts.set_index("method"))

    with col2:
    # Traffic Variation by Road Category (if available)
        if 'road_classification' in df.columns:
            st.markdown("<div class='section-header'> 🛤️ Traffic by Road Classification", unsafe_allow_html=True)
            category_traffic = df.groupby("road_classification")["total_traffic"].sum().reset_index()
            category_chart = alt.Chart(category_traffic).mark_bar().encode(
            x=alt.X("road_classification:N", sort="-y", title="Road Classification"),
            y=alt.Y("total_traffic:Q", title="Total Traffic"),
            tooltip=["road_classification", "total_traffic"]
        ).properties(height=400)
        st.altair_chart(category_chart, use_container_width=True)

    fixed_columns  = [  
        'count_point_id','year', 'count_method', 'pedal_cycles', 'two_wheeled_motor_vehicles', 'cars_and_taxis', 'buses_and_coaches', 'light_goods_vehicles', 'heavy_goods_vehicles','all_motor_vehicles',
        'pedal_cycles_yoy_growth','two_wheeled_motor_vehicles_yoy_growth','cars_and_taxis_yoy_growth','buses_and_coaches_yoy_growth','light_goods_vehicles_yoy_growth','heavy_goods_vehicles_yoy_growth','all_motor_vehicles_yoy_growth',
        'total_traffic','total_traffic_yoy_growth', 'region', 'local_authority', 'road_name', 'road_classification', 'managed_by', 'road_type', 'start_junction', 'end_junction', 'link_km', 'link_miles', 'easting', 'northing', 'latitude', 'longitude'
    ]

    view_df = df[[col for col in fixed_columns if col in df.columns]]

    # Data Table & Downloads
    st.markdown("<div class='section-header'>Download Dataset</div>", unsafe_allow_html=True)

    # Download button on the right
    col_left, col_right = st.columns([6, 1])
    with col_right:
        csv_data = view_df.to_csv(index=False).encode('utf-8')
        st.download_button(label="Download CSV",data=csv_data,file_name="traffic_data_filtered.csv",mime="text/csv",use_container_width=True)
    st.dataframe(view_df, use_container_width=True, height=500)
    
    

    

    
