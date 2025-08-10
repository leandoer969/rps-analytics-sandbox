# streamlit/app.py
import streamlit as st

st.set_page_config(page_title="RPS Analytics", page_icon="🧪", layout="wide")
st.title("RPS Analytics Sandbox")
st.write("Use the sidebar to navigate, or jump in below:")

st.sidebar.header("Navigation")
st.sidebar.page_link("app.py", label="🏠 Home")
st.sidebar.page_link("pages/01_Executive_Overview.py", label="📊 Executive Overview")
st.sidebar.page_link("pages/02_Forecast_vs_Actuals.py", label="📈 Forecast vs Actuals")
st.sidebar.page_link("pages/03_Brand_Performance.py", label="🏷️ Brand Performance")

cols = st.columns(3)
with cols[0]:
    st.page_link(
        "pages/01_Executive_Overview.py", label="Open Executive Overview", icon="📊"
    )
with cols[1]:
    st.page_link(
        "pages/02_Forecast_vs_Actuals.py", label="Open Forecast vs Actuals", icon="📈"
    )
with cols[2]:
    st.page_link(
        "pages/03_Brand_Performance.py", label="Open Brand Performance", icon="🏷️"
    )
