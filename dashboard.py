import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# --- CONFIGURATION (SECURE) ---
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# --- CONNECT TO SNOWFLAKE ---
@st.cache_data 
def load_data():
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=SF_ROLE
    )
    # query our CLEAN transformed data
    query = "SELECT * FROM STG_PRODUCTS"
    cur = conn.cursor()
    cur.execute(query)
    # Fetch result into a pandas DataFrame
    df = cur.fetch_pandas_all()
    return df

# --- PAGE LAYOUT ---
st.set_page_config(page_title="Modern BI Dashboard", layout="wide")
st.title("🛍️ Modern E-Commerce Dashboard")
st.markdown("Build with **Snowflake**, **dbt**, and **Streamlit**.")

try:
    # Load Data
    df = load_data()

    # 1. KPI SECTION
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Products", len(df))
    col2.metric("Average Price", f"${df['PRICE'].mean():.2f}")
    col3.metric("Avg Rating", f"{df['RATING_STARS'].mean():.1f} ⭐")

    st.divider()

    # 2. CHARTS SECTION
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.subheader("💰 Price by Category")
        # Group by category and get average price
        bar_data = df.groupby('CATEGORY')['PRICE'].mean()
        st.bar_chart(bar_data)

    with col_chart2:
        st.subheader("⭐ Ratings Distribution")
        st.bar_chart(df['RATING_STARS'].value_counts())

    # 3. RAW DATA SECTION
    st.subheader("📄 Raw Data Preview")
    st.dataframe(df)

except Exception as e:
    st.error(f"❌ Connection Error: {e}")
