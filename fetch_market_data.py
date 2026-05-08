#!/usr/bin/env python3
"""
Fetch live market data from Yahoo Finance v8 spark API.
Runs hourly via GitHub Actions.
"""
import json, urllib.request, urllib.parse, time, os

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
BATCH = 20

def fetch_spark(symbols):
    results = {}
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i+BATCH]
        url = "https://query1.finance.yahoo.com/v8/finance/spark?symbols={}&range=1d&interval=1d".format(
            urllib.parse.quote(",".join(batch)))
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
                for sym, info in data.items():
                    closes = info.get("close") or []
                    price  = closes[-1] if closes else None
                    prev   = info.get("chartPreviousClose")
                    chg    = round((price - prev) / prev * 100, 2) if price and prev else None
                    results[sym] = {"price": price, "chg": chg}
        except Exception as e:
            print(f"  WARN [{i//BATCH}]: {e}")
        time.sleep(0.35)
    return results

# ── S&P 500 (503 종목) ──────────────────────────────────────────────────────
SP500 = [
  "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","BRK.B","AVGO",
  "JPM","LLY","UNH","XOM","V","COST","MA","HD","PG","WMT",
  "NFLX","JNJ","CRM","BAC","ABBV","CVX","MRK","ORCL","KO","WFC",
  "ACN","PEP","TMO","CSCO","NOW","ABT","MCD","GE","LIN","DHR",
  "TXN","IBM","PM","CAT","ISRG","VZ","INTU","AMGN","SPGI","BKNG",
  "RTX","GS","HON","UBER","T","C","NEE","MS","BX","SCHW",
  "LOW","DE","UNP","SYK","BLK","AXP","PLD","TJX","MDT","MMC",
  "BMY","GILD","ADI","VRTX","AMAT","ADP","CB","ETN","CI","MDLZ",
  "SHW","LRCX","MO","SO","PGR","CL","ICE","DUK","BSX","CME",
  "KLAC","ZTS","REGN","MCO","PH","SNPS","EOG","CEG","NOC","ITW",
  "MSI","WM","FCX","EMR","APD","PSA","COF","TDG","GM","USB",
  "CDNS","MAR","HCA","CSX","HLT","ORLY","TT","ECL","NSC","GD",
  "HUM","PCAR","CMG","F","FDX","SLB","OXY","AFL","EL","AIG",
  "EW","ROST","MNST","NEM","AEP","CTAS","PAYX","BDX","KMB","PEG",
  "TFC","ALL","PSX","A","OKE","D","PRU","FTNT","MCHP","IDXX",
  "PWR","CCI","COP","MSCI","MTD","ODFL","KR","MPC","RCL","CARR",
  "VRSK","DHI","CSGP","YUM","EXC","SRE","CPRT","KDP","DLR","DLTR",
  "DOV","OTIS","KVUE","IQV","EA","ON","GIS","LHX","HIG","HPQ",
  "NUE","DAL","FAST","AME","CDW","ACGL","XEL","PPG","BR","URI",
  "DG","CMI","PTC","EFX","BK","WAT","CTSH","AVB","EBAY","ROL",
  "EXPD","SYY","RMD","STZ","TDY","IRM","GLW","ANSS","VLO","GWW",
  "TROW","EQR","HSY","TSCO","CBOE","HES","EIX","KEYS","FANG","HAL",
  "AWK","CINF","DFS","LUV","MTB","NTAP","VLTO","IT","CLX","NDAQ",
  "LDOS","TER","BIIB","BALL","STE","TRV","TPL","IP","UAL","MOH",
  "HOLX","AES","CF","MKC","PKG","ULTA","CMS","VRSN","BBY","ATO",
  "PODD","LYB","IEX","L","CHD","DTE","ES","ZBRA","NTRS","PFG",
  "ERIE","WY","BRO","LEN","NRG","TRMB","NDSN","RL","CHRW","FMC",
  "MAS","TAP","BBWI","PAYC","HRL","EPAM","WBA","MHK","LNC","DVN",
  "MTCH","NWS","NWSA","FOX","FOXA","BEN","XRAY","FRT","VFC",
  "PARA","CZR","ZION","CPB","AOS","UDR","AIZ","ETSY","HSIC","RE",
]

# ── KOSPI 200 ────────────────────────────────────────────────────────────────
KOSPI = [
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

# ── Nikkei 225 ───────────────────────────────────────────────────────────────
NIKKEI = [
  "7203.T","6758.T","8306.T","9432.T","8035.T","6861.T","9433.T","7267.T","4063.T","8411.T",
  "9984.T","6501.T","6902.T","6367.T","7741.T","2914.T","6702.T","8766.T","8031.T","6954.T",
  "4543.T","8001.T","7751.T","6971.T","5108.T","8802.T","7733.T","3382.T","7832.T","9022.T",
  "6503.T","4519.T","9021.T","6645.T","4901.T","8053.T","8058.T","9020.T","5401.T","4568.T",
  "7269.T","9201.T","4661.T","6762.T","8604.T","2502.T","3407.T","9101.T","6752.T","6301.T",
]

ts = time.time()
print(f"Fetching S&P 500 ({len(SP500)} symbols)...")
sp_data = fetch_spark(SP500)
print(f"  → {len(sp_data)} OK")

print(f"Fetching KOSPI ({len(KOSPI)} symbols)...")
ks_data = fetch_spark(KOSPI)
print(f"  → {len(ks_data)} OK")

print(f"Fetching Nikkei ({len(NIKKEI)} symbols)...")
nk_data = fetch_spark(NIKKEI)
print(f"  → {len(nk_data)} OK")

for fname, d in [("sp500_live.json", sp_data), ("kospi_live.json", ks_data), ("nikkei_live.json", nk_data)]:
    with open(fname, "w") as f:
        json.dump({"ts": ts, "data": d}, f, separators=(",",":"))
    print(f"  Saved {fname} ({os.path.getsize(fname):,} bytes)")

print("Done!")
