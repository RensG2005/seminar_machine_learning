import os
import time     
from edgar import Filing, set_identity, find, Company
from pathlib import Path

#set path
BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data" / "Sections"
data_dir.mkdir(parents=True, exist_ok=True)

#set identity (required)
# set_identity("Tony Hurioglu tony@trhur.com")
set_identity("Rens Gerritsen 658549rg@eur.nl")

#fetch filing
tickers = ["0000001750"]
start_yr = 2019

def download_filings():
    for ticker in tickers:
        print(f"\n--- processing {ticker} ---")
        try:
            company = Company(ticker)
            filings = company.get_filings(form="10-K")

            for filing in filings:
                year = filing.filing_date.year

                if year < start_yr:
                    continue
                
                strat_path = data_dir / f"{ticker}_{year}_strategy.txt"
                risk_path = data_dir / f"{ticker}_{year}_risk.txt"
                mgmt_path = data_dir / f"{ticker}_{year}_MD&A.txt"

                if strat_path.exists() and risk_path.exists():
                    print(f"Skipping {year} (already exists)")
                    continue

                print(f"Downloading {year} 10-K sections...")

                tenk = filing.obj()

                strategy_text = tenk['Item 1']
                risk_text = tenk['Item 1A']
                mgmt_text = tenk['Item 7']

                if strategy_text:
                    strat_path.write_text(strategy_text, encoding="utf-8")
                if risk_text:
                    risk_path.write_text(risk_text, encoding="utf-8")
                if mgmt_text:
                    mgmt_path.write_text(mgmt_text, encoding="utf-8")

                time.sleep(1)
        
        except Exception as e:
            print(f"Error with {ticker}: {e}")

if __name__ == "__main__":
    download_filings()
        
