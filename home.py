import streamlit as st
from streamlit_option_menu import option_menu
import base64
import traffic_page
import weather_page
import events_page
import logging
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

# ------------------------- STREAMLIT CONFIG -------------------------

# Set page config
st.set_page_config(page_title="Citisense Analytics", layout="wide", initial_sidebar_state="expanded")

# ------------------------- CSS -------------------------

st.markdown("""
<style>
    /* Import Montserrat Font */
    @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;600;700&display=swap');

    html, body, [class*="st-"], h1, h2, h3, h4, h5, h6, p, div, span, label, .css-10trblm, .css-1v0mbdj {
        font-family: 'Montserrat', sans-serif !important;
    }

    #MainMenu, footer, header {
        visibility: hidden;
    }
    
    h1 {
        text-align: center !important;
        margin-top: 2rem;
        color: #1e3a8a;
        font-weight: 700;
    }

    .sidebar-title {
        display: flex;
        align-items: center;
        gap: 12px;
        padding-bottom: 1rem;
    }

    .card-container {
        display: flex;
        flex-wrap: wrap;
        justify-content: flex-start;
        gap: 1.5rem;
        margin-top: 1rem;
    }

    .card {
        background-color: #f5f7fa;
        border-radius: 12px;
        padding: 1.5rem;
        flex: 1 1 280px;
        max-width: 100%;
        box-shadow: 0 4px 8px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
        cursor: pointer;
        text-align: center;
    }

    .card:hover {
        transform: translateY(-6px);
        background-color: #e0ebf5;
    }

    .card img {
        width: 60px;
        margin-bottom: 10px;
        margin-left: auto;
        margin-right: auto;
        display: block;
    }

    .card-title {
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 6px;
    }

    .card-text {
        font-size: 0.9rem;
        color: #475569;
    }

    .how-works li {
        margin-bottom: 8px;
    }

    /* Responsive design */
    @media screen and (max-width: 768px) {
        h1 {
            font-size: 1.8rem;
        }
        .card {
            padding: 1.2rem;
        }
        .card-title {
            font-size: 1rem;
        }
        .card-text {
            font-size: 0.85rem;
        }
    }

    @media screen and (max-width: 480px) {
        .card-container {
            flex-direction: column;
            gap: 1rem;
        }
        .card {
            width: 100%;
        }
    }
</style>
""", unsafe_allow_html=True)


# ------------------------- LOGO -------------------------

logo_path = "images/citisense_logo.svg"

# Convert logo to base64
with open(logo_path, "rb") as f:
    logo_data = base64.b64encode(f.read()).decode()

# Display logo + title
st.sidebar.markdown(f"""
    <div style="display: flex; align-items: center; gap: 10px; padding-bottom: 10px;">
        <img src="data:image/svg+xml;base64,{logo_data}" width="30"/>
        <h3 style="margin: 0; font-size: 20px;">Citisense Analytics</h3>
    </div>
""", unsafe_allow_html=True)
    
# ------------- NAVBAR -------------

from streamlit_option_menu import option_menu
with st.sidebar:
    selected = option_menu(
        menu_title=None,
        options=["Home", "Traffic", "Weather", "Events"],
        icons=["house", "car-front", "cloud-sun", "globe2"],
        menu_icon="cast",
        default_index=0,
    )

# ------------- PAGE ROUTING -------------
if selected == "Home":
    st.title("🏙️ Welcome to Citisense Analytics")
    st.markdown("""
    Citisense Analytics is a smart city dashboard integrating **UK road traffic data**, 
    **real-time & forecasted weather information**, and an **AI-based web scraper**. It empowers transport planning,
    city infrastructure monitoring, and policy evaluation with data-driven insights.
    """)
    st.markdown("### 🔍 Explore Modules", unsafe_allow_html=True)
    st.markdown("""
    <div class="card-container">
        <div class="card">
            <img src="https://cdn-icons-png.flaticon.com/512/854/854878.png">
            <div class="card-title">Traffic Analysis</div>
            <div class="card-text">Analyze road traffic flow by region, year and vehicle types.</div>
        </div>
        <div class="card">
            <img src="https://cdn-icons-png.flaticon.com/512/3845/3845731.png">
            <div class="card-title">Weather Insights</div>
            <div class="card-text">Explore temperature, wind, and rainfall trends for each borough.</div>
        </div>
        <div class="card">
            <img src="https://cdn-icons-png.flaticon.com/512/3039/3039396.png">
            <div class="card-title">Events Scraper</div>
            <div class="card-text">Extract event details from public listings and analyze their potential impact on traffic.</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

elif selected == "Traffic":
    with st.spinner("Loading traffic data..."):
        traffic_page.render_page()

elif selected == "Weather":
    with st.spinner("Loading weather data..."):
        weather_page.render_page()

elif selected == "Events":
    events_page.render_page()