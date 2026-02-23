import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
import streamlit.components.v1 as components
import numpy as np
# ------------------------- STREAMLIT CONFIG -------------------------

# Set page config
st.set_page_config(page_title="Citisense Weather Analytics", layout="wide", initial_sidebar_state="expanded")

# ------------------------- DATABASE SETUP -------------------------

def init_engine():
    connect_args = {"sslmode": "require"}
    engine2 = create_engine(st.secrets["db2"]["uri"],connect_args=connect_args)
    return engine2

engine2 = init_engine()

# ------------------------- DATA LOADING -------------------------

def fetch_data():
    query = "SELECT * FROM weather_forecasts;"
    with engine2.connect() as conn:
        df1 = pd.read_sql(text(query), conn)
    return df1

# ------------------------- AMCHARTS WRAPPER -------------------------

def amchart_div(id_str, chart_script, height=400):
    return f"""
        <div id="{id_str}" style="height:{height}px;"></div>
        <script src="https://cdn.amcharts.com/lib/5/index.js"></script>
        <script src="https://cdn.amcharts.com/lib/5/xy.js"></script>
        <script src="https://cdn.amcharts.com/lib/5/percent.js"></script>
        <script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>
        <script>am5.ready(function() {{{chart_script}}});</script>
    """
    
# ------------------------- MAIN DASHBOARD -------------------------

def render_page():
    df1 = fetch_data()
    df1['date'] = pd.to_datetime(df1['date'])
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
                
        /* KPI card styling */
        .kpi-card {
            background-color: #ffffff;
            border-radius: 8px;
            padding: 16px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
            text-align: center;
            margin-bottom: 16px;
        }
                
        .kpi-card:hover {
            transform: translateY(-6px);
            background-color: #e0ebf5;
        }
                
        .kpi-title {
            font-size: 1rem;
            font-weight: 600;
            color: #555;
            margin-bottom: 8px;
        }

        .kpi-value {
            font-size: 1.5rem;
            font-weight: bold;
            color: #111;
        }

        .kpi-value1 {
            font-size: 15px;
            font-weight: bold;
            color: #111;
        }
                
        .kpi-icon {
            margin-right: 4px;
        }

        .green { color: #0a0; }
        .red   { color: #a00; }
        .gray  { color: #888; }

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
            
            .section-header {
                font-size: 1.2rem;
            }
        } 
    </style> 
    """, unsafe_allow_html=True)

    # ------------------------- SIDEBAR FILTERS -------------------------

    all_boros = ["All Boroughs"] + sorted(df1['borough'].unique())
    sel_boro = st.sidebar.selectbox("Borough", all_boros)

    min_dt = df1['date'].min().date()
    max_dt = df1['date'].max().date()

    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_dt, max_dt),
        min_value=min_dt,
        max_value=max_dt
    )

    # safe unpack — if the user somehow picks only one date, treat start==end
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    # convert to Timestamps (at midnight)
    start_ts = pd.to_datetime(start_date)
    end_ts   = pd.to_datetime(end_date)

    # build one mask for both date-range and borough
    mask = (df1['date'] >= start_ts) & (df1['date'] <= end_ts)
    if sel_boro != "All Boroughs":
        mask &= (df1['borough'] == sel_boro)

    df1 = df1.loc[mask]
    
    # ------------------------- PREPROCESSING -------------------------

    df1['temp_range'] = df1['max_temp'] - df1['min_temp']
    df1['sunrise_datetime'] = pd.to_datetime(df1['sunrise_datetime'])
    df1['sunset_datetime'] = pd.to_datetime(df1['sunset_datetime'])
    df1['day_length_minutes'] = (df1['sunset_datetime'] - df1['sunrise_datetime']).dt.total_seconds() / 60

    hourly_temp_cols = [f"temp_{i}" for i in range(24) if f"temp_{i}" in df1.columns]
    df1[hourly_temp_cols] = df1[hourly_temp_cols].replace(r'°', '', regex=True).apply(pd.to_numeric, errors='coerce')
    hourly_temp_df = df1[hourly_temp_cols].mean().reset_index()
    hourly_temp_df.columns = ['hour', 'avg_temp']
    hourly_temp_df['hour'] = hourly_temp_df['hour'].str.extract('(\\d+)').astype(int)

    wind_speed_cols = [f"wind_{i}_speed" for i in range(24) if f"wind_{i}_speed" in df1.columns]
    hourly_wind_df = df1[wind_speed_cols].mean().reset_index()
    hourly_wind_df.columns = ['hour', 'avg_wind_speed']
    hourly_wind_df['hour'] = hourly_wind_df['hour'].str.extract('(\\d+)').astype(int)

    wind_dir_cols = [f"wind_{i}_dir" for i in range(24) if f"wind_{i}_dir" in df1.columns]
    wind_dir_mode = pd.Series(dtype='object')
    for col in wind_dir_cols:
        if col in df1.columns:
            wind_dir_mode = pd.concat([wind_dir_mode, df1[col].dropna().astype(str)])
    most_common_dirs = wind_dir_mode.value_counts().head(10).reset_index()
    most_common_dirs.columns = ['direction', 'count']

    temp_heatmap_df = df1[['borough'] + hourly_temp_cols].melt(id_vars=['borough'], 
                                                           var_name='hour', value_name='temp')
    temp_heatmap_df['hour'] = temp_heatmap_df['hour'].str.extract('(\\d+)').astype(int)

    sunrise_df = df1[['borough', 'sunrise_datetime', 'sunset_datetime']].copy()
    sunrise_df['sunrise_hour'] = sunrise_df['sunrise_datetime'].dt.hour + sunrise_df['sunrise_datetime'].dt.minute / 60
    sunrise_df['sunset_hour'] = sunrise_df['sunset_datetime'].dt.hour + sunrise_df['sunset_datetime'].dt.minute / 60

    # ------------------------- AGGREGATE DATA -------------------------

    hot10  = df1[['borough','max_temp']].sort_values('max_temp', ascending=False).head(10)
    hourly_t = pd.DataFrame({'hour':range(24), 'avg_temp':[df1[f'temp_{h}'].mean() for h in range(24)]})
    hourly_w = pd.DataFrame({'hour':range(24), 'avg_wind':[df1[f'wind_{h}_speed'].mean() for h in range(24)]})
    # Wind directions
    dirs = [c for c in df1.columns if c.endswith('_dir')]
    all_dirs = pd.Series(dtype=str)
    for c in dirs:
        series = df1[c].dropna().astype(str)
        all_dirs = pd.concat([all_dirs, series], ignore_index=True)

    top_dirs = all_dirs.value_counts().head(10).reset_index()
    top_dirs.columns = ['direction','count']

    # ------------------------- DETERMINING GROUPING COLUMN -------------------------

    possible_groups = ['borough', 'day_label', 'date']
    # fallback to first match in available columns
    group_col = next((c for c in possible_groups if c in df1.columns), None)

    # ------------------------- SUMMARY STATISTICS -------------------------

    st.title("🌤️ Weather Forecast Analytics Dashboard")
    st.subheader("London Borough Summary")

    # Total london boroughs
    num_groups = df1[group_col].dropna().nunique() if group_col in df1.columns else 0
    label_metric = group_col.replace('_',' ').title() + 's' if group_col in df1.columns else 'Groups'

    # Avg Temp Range
    avg_range = df1['temp_range'].mean()    
    
    # Avg Max Temp Overall
    avg_max = df1['max_temp'].mean()
    
    col1, col2, col3, col4 = st.columns(4, gap='large')
    col1.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🗺️</span>Total Boroughs</div><div class='kpi-value'>{num_groups}</div></div>""", unsafe_allow_html=True)

    col2.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🌡️</span>Avg Temp Range</div><div class='kpi-value'>{avg_range:.1f} °C</div></div>""", unsafe_allow_html=True)
    
    # Peak hour (hottest average hour)
    if not hourly_t['avg_temp'].isna().all():
        peak_hour = int(hourly_t.loc[hourly_t['avg_temp'].idxmax(), 'hour'])
        col3.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>☀️</span>Peak Heat Hour (avg)</div><div class='kpi-value'>{peak_hour}:00</div></div>", unsafe_allow_html=True)
    else:
        col3.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>☀️</span>Peak Heat Hour (avg)</div><div class='kpi-value'>—</div></div>", unsafe_allow_html=True)

    col4.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🔥</span>Avg Max Temp</div><div class='kpi-value'>{avg_max:.1f} °C</div></div>""", unsafe_allow_html=True)
    
    # Rainy Day Count
    rainy_count = df1[df1['summary'].str.contains('Rain', case=False, na=False)].shape[0]
    
    # Sunny Day Count
    sunny_count = df1[df1['summary'].str.contains('Sunny', case=False, na=False)].shape[0]
    
    # Cloudy Day Count
    cloudy_count = df1[df1['summary'].str.contains('Cloudy', case=False, na=False)].shape[0]
    
    # High Wind Days (> 20 mph max wind)
    max_wind_speed_row = df1[[f'wind_{h}_speed' for h in range(24)]].max(axis=1)
    high_wind_days = df1[max_wind_speed_row > 20].shape[0]
    
    st.markdown("---")

    col5, col6, col7, col8 = st.columns(4, gap='large')
    col5.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🌧️</span>Rainy Intervals</div><div class='kpi-value'>{rainy_count}</div></div>""", unsafe_allow_html=True)
    
    col6.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>☀️</span>Sunny Intervals</div><div class='kpi-value'>{sunny_count}</div></div>""", unsafe_allow_html=True)
    
    col7.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>☁️</span>Cloudy Intervals</div><div class='kpi-value'>{cloudy_count}</div></div>""", unsafe_allow_html=True)
    
    col8.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>💨</span>High Wind Intervals</div><div class='kpi-value'>{high_wind_days}</div></div>""", unsafe_allow_html=True)
    
    st.markdown("---")

    col9, col10, col11, col12 = st.columns(4, gap='large')
    if 'max_temp' in df1 and not df1['max_temp'].isna().all():
        hot_idx = df1['max_temp'].idxmax()
        hot_b = df1.loc[hot_idx, 'borough']
        hot_d = df1.loc[hot_idx, 'date'].strftime('%Y-%m-%d')
        col9.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🔥</span>Hottest (Max Temp)</div><div class='kpi-value1'>{hot_b} - {df1.loc[hot_idx,'max_temp']:.1f} °C</div><div>{hot_d}</div></div>", unsafe_allow_html=True)
    else:
        col9.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🔥</span>Hottest (Max Temp)</div><div class='kpi-value1'>—</div></div>", unsafe_allow_html=True)

    if 'min_temp' in df1 and not df1['min_temp'].isna().all():
        cold_idx = df1['min_temp'].idxmin()
        cold_b = df1.loc[cold_idx, 'borough']
        cold_d = df1.loc[cold_idx, 'date'].strftime('%Y-%m-%d')
        col10.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>❄️</span>Coldest (Min Temp)</div><div class='kpi-value1'>{cold_b} - {df1.loc[cold_idx,'min_temp']:.1f} °C</div><div>{cold_d}</div></div>", unsafe_allow_html=True)
    else:
        col10.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>❄️</span>Coldest (Min Temp)</div><div class='kpi-value1'>—</div></div>", unsafe_allow_html=True)

    if 'day_length_minutes' in df1 and not df1['day_length_minutes'].isna().all():
        long_idx = df1['day_length_minutes'].idxmax()
        long_b = df1.loc[long_idx, 'borough']
        long_d = df1.loc[long_idx, 'date'].strftime('%Y-%m-%d')
        col11.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🌅</span>Longest Day</div><div class='kpi-value1'>{long_b} - {df1.loc[long_idx,'day_length_minutes']:.0f} min</div><div>{long_d}</div></div>", unsafe_allow_html=True)
    else:
        col11.markdown(f"<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🌅</span>Longest Day</div><div class='kpi-value1'>—</div></div>", unsafe_allow_html=True)

    # Highest Wind Speed Borough + Date
    max_wind = df1[[f'wind_{h}_speed' for h in range(24)]].max(axis=1)
    max_wind_idx = max_wind.idxmax()
    w_b = df1.loc[max_wind_idx, 'borough']
    w_val = max_wind.max()
    w_d = df1.loc[max_wind_idx, 'date'].strftime('%Y-%m-%d')
    col12.markdown(f"""<div class='kpi-card'><div class='kpi-title'><span class='kpi-icon'>🌪️</span>Max Wind Speed</div><div class='kpi-value1'>{w_b} - {w_val:.1f} mph</div><div>{w_d}</div></div>""", unsafe_allow_html=True)

    # ------------------------- AVERAGE DAILY TEMPERATURE BY BOROUGH -------------------------

    if group_col and 'max_temp' in df1.columns and 'min_temp' in df1.columns:
        label = group_col.replace('_', ' ').title()
        st.markdown(f'<div class="section-header">Average Daily Temps by {label}</div>', unsafe_allow_html=True)
        avg_temps = df1.groupby(group_col)[['max_temp', 'min_temp']].mean().reset_index()
        chart_avg = {
            "type": "serial",
            "theme": "light",
            "dataProvider": avg_temps.to_dict(orient="records"),
            "valueAxes": [{"unit": "°C", "position": "left"}],
            "graphs": [
                {"balloonText": "[[value]] °C", "valueField": "max_temp", "title": "Avg Max", "fillAlphas": 0.7},
                {"balloonText": "[[value]] °C", "valueField": "min_temp", "title": "Avg Min", "fillAlphas": 0.7}
            ],
            "categoryField": group_col,
            "categoryAxis": {"labelRotation": 45}
        }
        json_avg = json.dumps(chart_avg)
        html_avg = f"""
            <div id="avg_chart" style="width:100%; height:400px;"></div>
            <script src="https://cdn.amcharts.com/lib/3/amcharts.js"></script>
            <script src="https://cdn.amcharts.com/lib/3/serial.js"></script>
            <script src="https://cdn.amcharts.com/lib/3/themes/light.js"></script>
            <script>AmCharts.makeChart("avg_chart", {json_avg});</script>
            """
        components.html(html_avg, height=450)
    else:
        st.warning("Required columns for average temperature chart not found; skipping.")

    # ------------------------- HOURLY AVERAGE TEMPERATURE (°C) VS AVERAGE WIND (mph) -------------------------

    st.markdown('<div class="section-header">Hourly Average Temperature (°C) vs Wind (mph)</div>', unsafe_allow_html=True)
    combo = []
    for h in range(24):
        tcol = f"temp_{h}"
        wcol = f"wind_{h}_speed"
        if tcol in df1.columns and wcol in df1.columns:
            t = float(df1[tcol].mean()) if not df1[tcol].isna().all() else None
            w = float(df1[wcol].mean()) if not df1[wcol].isna().all() else None
            if t is not None or w is not None:
                combo.append({"hour": h, "temp": t, "wind": w})

    if len(combo) == 0:
        st.info("No hourly temperature/wind data available for this selection.")
    else:
        combo_js = json.dumps(combo)
        script_dual = f"""
            var root = am5.Root.new('dual');
            root.setThemes([am5themes_Animated.new(root)]);
            var chart = root.container.children.push(am5xy.XYChart.new(root, {{layout: root.verticalLayout,wheelX: "panX", wheelY: "zoomX", panX: true}}));
            // Axes
            var xAxis = chart.xAxes.push(am5xy.CategoryAxis.new(root, {{categoryField: "hour",renderer: am5xy.AxisRendererX.new(root, {{ minGridDistance: 25 }})}}));
            xAxis.data.setAll({combo_js});
            var yLeft  = chart.yAxes.push(am5xy.ValueAxis.new(root, {{renderer: am5xy.AxisRendererY.new(root, {{}}),numberFormat: "#.#",extraMax: 0.1,tooltip: am5.Tooltip.new(root, {{ labelText: "°C" }})}}));
            var yRight = chart.yAxes.push(am5xy.ValueAxis.new(root, {{renderer: am5xy.AxisRendererY.new(root, {{ opposite: true }}),numberFormat: "#.#",extraMax: 0.2,tooltip: am5.Tooltip.new(root, {{ labelText: "mph" }})}}));
            // Wind columns (right axis)
            var wind = chart.series.push(am5xy.ColumnSeries.new(root, {{name: "Wind",xAxis: xAxis, yAxis: yRight,categoryXField: "hour", valueYField: "wind",tooltip: am5.Tooltip.new(root, {{ labelText: "Wind: {{valueY.formatNumber('#.#')}} mph" }})}}));
            wind.data.setAll({combo_js});
            // Temp line (left axis)
            var temp = chart.series.push(am5xy.LineSeries.new(root, {{name: "Temp",xAxis: xAxis, yAxis: yLeft,categoryXField: "hour", valueYField: "temp",strokeWidth: 2,tooltip: am5.Tooltip.new(root, {{ labelText: "Temp: {{valueY.formatNumber('#.#')}} °C" }})}}));
            temp.bullets.push(() => am5.Bullet.new(root, {{sprite: am5.Circle.new(root, {{ radius: 4 }})}}));
            temp.data.setAll({combo_js});
            chart.set("cursor", am5xy.XYCursor.new(root, {{ behavior: "none" }}));
            temp.appear(); wind.appear(); chart.appear(800, 80);
        """
        components.html(amchart_div("dual", script_dual), height=460)


    # ------------------------- WIND SPEED DISTRIBUTION -------------------------
    col1,col2 = st.columns(2, gap="medium")
    with col1:
        st.markdown('<div class="section-header">Wind Speed Distribution</div>', unsafe_allow_html=True)
        if wind_speed_cols:
            row_max_wind = df1[wind_speed_cols].max(axis=1)
            if row_max_wind.notna().any():
                bins = list(range(int(row_max_wind.min())-1, int(row_max_wind.max())+2, 2))
                cats = pd.cut(row_max_wind, bins=bins, include_lowest=True)
                hist = cats.value_counts().sort_index().reset_index()
                hist.columns = ['bin','count']
                hist['label'] = hist['bin'].astype(str)
                hist_js = json.dumps(hist[['label','count']].to_dict('records'))
                script_hist = f"""
                    var root = am5.Root.new('windHist');
                    root.setThemes([am5themes_Animated.new(root)]);
                    var chart = root.container.children.push(am5xy.XYChart.new(root, {{}}));
                    var xAxis = chart.xAxes.push(am5xy.CategoryAxis.new(root, {{categoryField: "label",renderer: am5xy.AxisRendererX.new(root, {{ minGridDistance: 20, labels: {{ rotation: -30 }} }})}}));
                    var yAxis = chart.yAxes.push(am5xy.ValueAxis.new(root, {{ renderer: am5xy.AxisRendererY.new(root, {{}}) }}));
                    var s = chart.series.push(am5xy.ColumnSeries.new(root, {{xAxis: xAxis, yAxis: yAxis,valueYField: "count",categoryXField: "label",tooltip: am5.Tooltip.new(root, {{ labelText: "{{categoryX}}: {{valueY}}" }})}}));
                    var data = {hist_js};
                    xAxis.data.setAll(data); s.data.setAll(data);
                    s.columns.template.setAll({{ cornerRadiusTL: 4, cornerRadiusTR: 4 }});
                    chart.appear(1000, 100);
                """
                components.html(amchart_div('windHist', script_hist), height=420)
            else:
                st.info("No wind speed values available to plot.")
        else:
            st.info("Wind speed columns not found.")

    # ------------------------- SUMMARY BREAKDOWN -------------------------
    with col2:
        if 'summary' in df1:
            st.markdown('<div class="section-header">Weather Summary Breakdown</div>', unsafe_allow_html=True)
            cats = pd.Series(np.where(df1['summary'].str.contains('Rain', case=False, na=False), 'Rain',
                         np.where(df1['summary'].str.contains('Sunny', case=False, na=False), 'Sunny',
                         np.where(df1['summary'].str.contains('Cloud', case=False, na=False), 'Cloudy', 'Other'))))
            pie = cats.value_counts().reset_index()
            pie.columns = ['category','count']
            pie_js = json.dumps(pie.to_dict('records'))
            script_pie = f"""
                var root = am5.Root.new('sumPie');
                root.setThemes([am5themes_Animated.new(root)]);
                var chart = root.container.children.push(am5percent.PieChart.new(root, {{ endAngle: 360 }}));
                var series = chart.series.push(am5percent.PieSeries.new(root, {{categoryField: "category", valueField: "count", endAngle: 360 }}));
                series.data.setAll({pie_js});
                series.slices.template.setAll({{ tooltipText: "{{category}}: {{value}}" }});
                chart.appear(1000, 100);
            """
            components.html(amchart_div('sumPie', script_pie, height=380), height=400)

    