# etf_data_extract.py

import yfinance as yf
import pandas as pd
from pathlib import Path

# ----------------------------
# 1️⃣ Define ETFs and date range
# ----------------------------
etfs = {
    "SPY": "S&P 500 ETF",
    "QQQ": "Nasdaq 100 ETF",
    "IWM": "Russell 2000 ETF",
    "EFA": "MSCI EAFE ETF",
    "EEM": "MSCI Emerging Markets ETF"
}

start_date = "2020-01-01"
end_date = "2025-08-10"

# ----------------------------
# 2️⃣ Create data folder
# ----------------------------
data_folder = Path("data")
data_folder.mkdir(exist_ok=True)

# ----------------------------
# 3️⃣ Download ETF data
# ----------------------------
for ticker, name in etfs.items():
    print(f"Pulling data for {ticker} ({name})...")
    
    # Download historical daily data
    data = yf.download(ticker, start=start_date, end=end_date)
    
    # Add a ticker column
    data["Ticker"] = ticker
    
    # Reset index to make 'Date' a column
    data = data.reset_index()
    
    # Save to CSV
    csv_path = data_folder / f"{ticker}_historical.csv"
    data.to_csv(csv_path, index=False)
    
    print(f"Saved {csv_path}")

print("✅ All ETF data pulled and saved locally.")
