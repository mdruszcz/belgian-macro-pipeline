import urllib.request
import json

tickers = ["^BFX", "EURUSD=X", "^BE10Y", "BE10Y=X", "^DE10Y", "DE10Y=X", "ECB", "^ECB", "EURIBOR=X"]
headers = {'User-Agent': 'Mozilla/5.0'}
url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(tickers)}"

try:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
        if "quoteResponse" in data and "result" in data["quoteResponse"]:
            for quote in data["quoteResponse"]["result"]:
                print(f"Found: {quote['symbol']} - {quote.get('shortName')} - {quote.get('regularMarketPrice')}")
except Exception as e:
    print(f"Error: {e}")
