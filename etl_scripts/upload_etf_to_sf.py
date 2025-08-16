import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

# Load credentials from .env
load_dotenv()

sf_user = os.getenv('SNOWFLAKE_USER')
sf_password = os.getenv('SNOWFLAKE_PASSWORD')
sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
sf_warehouse = 'etf_wh'
sf_database = 'etf_db'
sf_schema = 'raw'

# Folder with CSVs
csv_folder = './etf_prices_raw'

# CSV → target table mapping
etf_files = {
    'SPY_historical.csv': 'SPY_historical',
    'QQQ_historical.csv': 'QQQ_historical',
    'EEM_historical.csv': 'EEM_historical',
    'IWM_historical.csv': 'IWM_historical',
    'EFA_historical.csv': 'EFA_historical'
}

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)
cs = conn.cursor()

# Make sure the session is using the right DB and schema
cs.execute(f'USE DATABASE {sf_database}')
cs.execute(f'USE SCHEMA {sf_schema}')

for file_name, table_name in etf_files.items():
    file_path = os.path.join(csv_folder, file_name)
    print(f"📂 Uploading {file_name} → {table_name}...")

    # Load CSV
    df = pd.read_csv(file_path)

    # Convert columns
    df['Date'] = pd.to_datetime(df['Date'])
    price_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    for col in price_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Create table if not exists
    col_defs = ['"Date" DATE'] + [f'"{col}" FLOAT' for col in price_cols if col in df.columns]
    cs.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)});')

    # Upload to Snowflake
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name=table_name, database=sf_database, schema=sf_schema)
    if success:
        print(f"✅ {nrows} rows inserted into {table_name}")
    else:
        print(f"❌ Failed to insert {table_name}")

cs.close()
conn.close()
