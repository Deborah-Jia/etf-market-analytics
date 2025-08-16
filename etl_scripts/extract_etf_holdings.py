import os
import pandas as pd
import requests
from datetime import datetime

# ==== CONFIG ====
OUTPUT_DIR = "etf_holdings_daily"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Tickers and their iShares / SSGA URLs
ETFS = {
    "SPY": "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
    "QQQ": "https://www.ishares.com/us/products/239726/ishares-nasdaq-100-etf/1467271812596.ajax",
    "IWM": "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax",
    "EFA": "https://www.ishares.com/us/products/239612/ishares-msci-efa-etf/1467271812596.ajax",
    "EEM": "https://www.ishares.com/us/products/239637/ishares-msci-emerging-markets-etf/1467271812596.ajax"
}

def download_holdings(ticker, url):
    snapshot_date = datetime.today().strftime("%Y-%m-%d")
    
    try:
        r = requests.get(url)
        r.raise_for_status()
        
        if ticker == "SPY":
            # Standard Excel file
            df = pd.read_excel(r.content, engine='openpyxl')
        else:
            # Use HTML table parsing for iShares ETFs
            tables = pd.read_html(r.text)
            if not tables:
                raise ValueError("No tables found on page")
            # Pick the largest table (most rows)
            df = max(tables, key=lambda x: x.shape[0])
        
        df['snapshot_date'] = snapshot_date
        
        # Save CSV locally
        csv_path = os.path.join(OUTPUT_DIR, f"{ticker}_holdings_{snapshot_date}.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ {ticker} holdings downloaded and saved as {csv_path}")
        
    except Exception as e:
        print(f"❌ Failed to get {ticker} holdings: {e}")

# ==== RUN EXTRACTION ====
for ticker, url in ETFS.items():
    download_holdings(ticker, url)
# ==== END OF SCRIPT ====