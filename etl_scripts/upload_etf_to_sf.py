import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load credentials from .env
load_dotenv("/Users/deborah_j/Documents/CEU/Projects_Showcase/proj1_ETF_Mkt_Pipeline/.env")

sf_user = os.getenv('SNOWFLAKE_USER')
sf_password = os.getenv('SNOWFLAKE_PASSWORD')
sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
sf_warehouse = 'ETF_WH'
sf_database = 'ETF_DB'
sf_schema = 'SILVER'

# Folder with CSVs
csv_folder = '/Users/deborah_j/Documents/CEU/Projects_Showcase/proj1_ETF_Mkt_Pipeline/data'

# CSV → target table mapping
etf_files = {
    'SPY_historical.csv': 'SPY_historical',
    'QQQ_historical.csv': 'QQQ_historical',
    'EEM_historical.csv': 'EEM_historical',
    'IWM_historical.csv': 'IWM_historical',
    'EFA_historical.csv': 'EFA_historical'
}

# Create SQLAlchemy engine
engine = create_engine(
    f'snowflake://{sf_user}:{sf_password}@{sf_account}/{sf_database}/{sf_schema}?warehouse={sf_warehouse}'
)

with engine.begin() as conn:  # Auto-commit transactions
    for file_name, table_name in etf_files.items():
        file_path = os.path.join(csv_folder, file_name)
        print(f"📂 Uploading {file_name} → {table_name}...")

        # Load CSV
        df = pd.read_csv(file_path)

        # Convert columns
        df['Date'] = pd.to_datetime(df['Date'])
        price_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in price_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Ensure table exists: build simple CREATE TABLE IF NOT EXISTS
        col_defs = ['"Date" DATE'] + [f'"{col}" FLOAT' for col in price_cols if col in df.columns]
        conn.execute(text(f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)})'))

        # Upload DataFrame to Snowflake
        df.to_sql(table_name, con=conn, if_exists='append', index=False, method='multi')
        print(f"✅ {len(df)} rows inserted into {table_name}")

print("All CSVs uploaded successfully!")
