import streamlit as st
import snowflake.connector
import pandas as pd

# --- CONFIGURATION (Same as your extraction script) ---
SF_ACCOUNT = 'aq12666.af-south-1.aws'
SF_USER = 'AYOUB23'
SF_PASSWORD = 'Ayoub@20050222'  
SF_WAREHOUSE = 'COMPUTE_WH'
SF_DATABASE = 'ECOMMERCE_DB'
SF_SCHEMA = 'PUBLIC'

# --- CONNECT TO SNOWFLAKE ---
@st.cache_data 
def load_data():
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
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
