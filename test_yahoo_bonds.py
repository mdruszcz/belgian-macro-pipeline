import urllib.request
import json
import time

tickers = ["^BE10Y", "^DE10Y", "BE10Y.F", "DE10Y.F", "%5ETNX", "%5EFVX"]
headers = {'User-Agent': 'Mozilla/5.0'}

for ticker in tickers:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            if "chart" in data and "result" in data["chart"] and data["chart"]["result"]:
                meta = data["chart"]["result"][0]["meta"]
                price = meta.get("regularMarketPrice")
                print(f"Success {ticker}: {price} {meta.get('currency')}")
            else:
                print(f"Empty {ticker}")
    except Exception as e:
        print(f"Error {ticker}: {e}")
    time.sleep(1)
