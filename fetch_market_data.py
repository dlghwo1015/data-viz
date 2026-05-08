#!/usr/bin/env python3
"""Fetch live market data from Yahoo Finance v8 spark API and save as JSON."""
import json, urllib.request, time, os

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

def fetch_spark(symbols):
    """Fetch price + prevClose for a list of symbols (max 200 at once)."""
    results = {}
    batch_size = 100
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        syms = ",".join(batch)
        url = f"https://query1.finance.yahoo.com/v8/finance/spark?symbols={urllib.parse.quote(syms)}&range=1d&interval=1d"
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
                for sym, info in data.items():
                    closes = info.get("close", []) or []
                    price = closes[-1] if closes else None
                    prev  = info.get("chartPreviousClose")
                    chg   = round((price - prev) / prev * 100, 2) if price and prev else None
                    results[sym] = {"price": price, "chg": chg}
        except Exception as e:
            print(f"  WARN batch {i}: {e}")
        if i + batch_size < len(symbols):
            time.sleep(0.3)
    return results

import urllib.parse

# ---- S&P 500 ----
sp500_tickers = [
  "AAPL","MSFT","NVDA","AVGO","ORCL","CRM","ADBE","ACN","NOW","AMD",
  "PANW","INTU","QCOM","TXN","AMAT","LRCX","KLAC","MU","SNPS","CDNS",
  "GOOGL","GOOG","META","AMZN","NFLX","DIS","CMCSA","CHTR","VZ","T",
  "TMUS","EA","TTWO","MTCH","PARA","WBD","LYV","IPG","OMC","FOX",
  "JPM","BAC","WFC","GS","MS","BLK","SCHW","AXP","USB","C",
  "PNC","TFC","COF","MCO","SPGI","ICE","CME","BX","APO","KKR",
  "UNH","LLY","JNJ","ABBV","MRK","ABT","TMO","DHR","AMGN","VRTX",
  "REGN","GILD","CVS","CI","HUM","ELV","ISRG","ZBH","BSX","BDX",
  "XOM","CVX","COP","SLB","EOG","PXD","PSX","VLO","MPC","HES",
  "TSLA","GM","F","TM","RIVN","LCID","HON","RTX","LMT","BA",
  "GE","CAT","MMM","EMR","ETN","PH","ROK","DOV","ITW","GD",
  "UPS","FDX","CSX","NSC","UNP","DAL","UAL","AAL","LUV","JBLU",
  "WMT","AMZN","COST","TGT","HD","LOW","BBY","DG","DLTR","KR",
  "PG","KO","PEP","PM","MO","MDLZ","GIS","K","CAG","HRL",
  "LIN","APD","SHW","DD","DOW","PPG","ECL","NEM","FCX","NUE",
  "NEE","DUK","SO","D","AEP","EXC","XEL","WEC","ES","ETR",
  "AMT","PLD","CCI","EQIX","SPG","PSA","DLR","WELL","EQR","VTR",
]
print("Fetching S&P 500...")
sp500_data = fetch_spark(sp500_tickers)
with open("sp500_live.json", "w") as f:
    json.dump({"ts": time.time(), "data": sp500_data}, f)
print(f"  S&P 500: {len(sp500_data)} symbols")

# ---- KOSPI ----
kospi_tickers = [
  "005930.KS","000660.KS","373220.KS","207940.KS","005380.KS",
  "000270.KS","068270.KS","105560.KS","051910.KS","055550.KS",
  "035420.KS","003550.KS","032830.KS","017670.KS","066570.KS",
  "010130.KS","009150.KS","028260.KS","015760.KS","096770.KS",
  "034730.KS","011170.KS","030200.KS","003490.KS","009830.KS",
  "033780.KS","012330.KS","018260.KS","002380.KS","010950.KS",
  "047050.KS","086790.KS","010140.KS","000810.KS","051900.KS",
  "161390.KS","000100.KS","001800.KS","097950.KS","139480.KS",
  "036570.KS","011780.KS","034020.KS","019880.KS","352820.KS",
  "006400.KS","000720.KS","004020.KS","326030.KS","003670.KS",
]
print("Fetching KOSPI...")
kospi_data = fetch_spark(kospi_tickers)
with open("kospi_live.json", "w") as f:
    json.dump({"ts": time.time(), "data": kospi_data}, f)
print(f"  KOSPI: {len(kospi_data)} symbols")

# ---- Nikkei ----
nikkei_tickers = [
  "7203.T","6758.T","8306.T","9432.T","8035.T","6861.T","9433.T","7267.T","4063.T","8411.T",
  "9984.T","6501.T","6902.T","6367.T","7741.T","2914.T","6702.T","8766.T","8031.T","6954.T",
  "4543.T","8001.T","7751.T","6971.T","5108.T","8802.T","7733.T","3382.T","7832.T","9022.T",
  "6503.T","4519.T","9021.T","6645.T","4901.T","8053.T","8058.T","9020.T","5401.T","4568.T",
  "7269.T","9201.T","4661.T","6762.T","8604.T","2502.T","3407.T","9101.T","6752.T","6301.T",
]
print("Fetching Nikkei...")
nikkei_data = fetch_spark(nikkei_tickers)
with open("nikkei_live.json", "w") as f:
    json.dump({"ts": time.time(), "data": nikkei_data}, f)
print(f"  Nikkei: {len(nikkei_data)} symbols")

print("Done!")
