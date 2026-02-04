import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv

# Load environment variables (only works locally if .env exists)
load_dotenv()

def run_pipeline():
    # 1. EXTRACT
    print("🚀 Fetching data from API...")
    response = requests.get("https://fakestoreapi.com/products")
    data = response.json()
    df = pd.DataFrame(data)
    
    # Fix the rating column
    if 'rating' in df.columns:
        df['rating'] = df['rating'].astype(str)
        
    print(f"✅ Extracted {len(df)} rows.")

    # 2. CONNECT (Using Secrets)
    engine = create_engine(URL(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA")
    ))

    # 3. LOAD
    try:
        connection = engine.connect()
        print("📦 Loading data into Snowflake...")
        df.to_sql('products_raw', con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)
        print("✅ Success! Data loaded.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        connection.close()
        engine.dispose()

if __name__ == "__main__":
    run_pipeline()