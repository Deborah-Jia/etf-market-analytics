import pandas as pd
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

# Snowflake connection parameters
sf_user = 'YOUR_USERNAME'
sf_password = 'YOUR_PASSWORD'
sf_account = 'YOUR_ACCOUNT'  # e.g., 'abcd1234.us-east-1'
sf_warehouse = 'YOUR_WAREHOUSE'
sf_database = 'YOUR_DATABASE'
sf_schema = 'YOUR_SCHEMA'

# Folder where your CSVs are stored
csv_folder = '/Users/deborah_j/Documents/CEU/Projects_cv/proj1_etf_Mkt_pipeline/data'  # adjust if needed

# Mapping of CSV filenames to target table names
etf_files = {
    'SPY_historical.csv': 'SPY_raw',
    'QQQ_historical.csv': 'QQQ_raw',
    'EEM_historical.csv': 'EEM_raw',
    'IWM_historical.csv': 'IWM_raw',
    'EFA_historical.csv': 'EFA_raw'
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

for file_name, table_name in etf_files.items():
    file_path = os.path.join(csv_folder, file_name)
    print(f"Uploading {file_name} to table {table_name}...")

    # Read CSV
    df = pd.read_csv(file_path)

    # Standardize column names
    df.columns = [c.replace(' ', '_') for c in df.columns]

    # Attempt to convert common columns to correct types
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    for col in ['Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Create table with proper types
    col_defs = []
    for col in df.columns:
        if col == 'Date':
            col_defs.append(f'"{col}" DATE')
        elif col in ['Open', 'High', 'Low', 'Close', 'Adj_Close']:
            col_defs.append(f'"{col}" FLOAT')
        elif col == 'Volume':
            col_defs.append(f'"{col}" BIGINT')
        else:
            col_defs.append(f'"{col}" STRING')
    cols_sql = ', '.join(col_defs)
    cs.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});')

    # Upload data
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
    if success:
        print(f"✅ {nrows} rows inserted into {table_name}")
    else:
        print(f"❌ Failed to insert {table_name}")

# Close connection
cs.close()
conn.close()
