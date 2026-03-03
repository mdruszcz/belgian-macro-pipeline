import urllib.request
import json
import time
import requests

stock_list = [
    {"id": "BEL20", "ticker": "^BFX", "name": "BEL 20"},
    {"id": "EURUSD", "ticker": "EURUSD=X", "name": "EUR/USD"},
]

def fetch_data():
    results = {}
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 1. Fetch Yahoo Data (Stocks/Forex)
    for stock in stock_list:
        try:
            # Try chart API for robustness
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock['ticker']}?interval=1d&range=5d"
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                meta = data["chart"]["result"][0]["meta"]
                price = meta.get("regularMarketPrice")
                prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
                
                # Fallback to indicators if meta price is missing
                if price is None and "indicators" in data["chart"]["result"][0]:
                    adjclose = data["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]
                    valid_closes = [x for x in adjclose if x is not None]
                    if valid_closes:
                        price = valid_closes[-1]
                        if prev_close is None and len(valid_closes) > 1:
                            prev_close = valid_closes[-2]

                if price is not None:
                    change = 0
                    if prev_close:
                        change = ((price - prev_close) / prev_close) * 100
                    
                    currency = meta.get("currency", "EUR")
                    if stock["id"] == "EURUSD": currency = "USD" # Fix for display

                    results[stock["id"]] = {
                        "name": stock["name"],
                        "ticker": stock["ticker"], # Use symbol as ticker text
                        "price": price,
                        "change": change,
                        "currency": currency
                    }
            time.sleep(0.5)
        except Exception as e:
            print(f"Error fetching {stock['id']}: {e}")

    # 2. Fetch DBnomics Data (Bonds & Rates)
    # Using Eurostat for Bonds and ECB for rates
    # BE 10Y: Eurostat/irt_lt_mcby_d/D.MCBY.BE (Daily Maastricht criterion bond yields)
    # DE 10Y: Eurostat/irt_lt_mcby_d/D.MCBY.DE
    # ECB MRO: ECB/FM/D.U2.EUR.4F.KR.MRR_FR.LEV (Main Refinancing Rate)
    
    db_series = {
        "BE_10Y": {"url": "https://api.db.nomics.world/v22/series/Eurostat/irt_lt_mcby_d/D.MCBY.BE?observations=true", "name": "BE OLO 10Y", "ticker": "BE10Y"},
        "DE_10Y": {"url": "https://api.db.nomics.world/v22/series/Eurostat/irt_lt_mcby_d/D.MCBY.DE?observations=true", "name": "DE Bund 10Y", "ticker": "DE10Y"},
        "ECB_RATE": {"url": "https://api.db.nomics.world/v22/series/ECB/FM/D.U2.EUR.4F.KR.MRR_FR.LEV?observations=true", "name": "ECB Rate", "ticker": "MRO"},
    }
    
    be_val = None
    de_val = None
    
    for key, info in db_series.items():
        try:
            r = requests.get(info["url"], timeout=10)
            if r.status_code == 200:
                data = r.json()
                series = data["series"]["docs"][0]
                values = series["value"]
                # Get last valid value
                valid_vals = [v for v in values if v is not None]
                if valid_vals:
                    curr = valid_vals[-1]
                    # Calculate change from previous value
                    prev = valid_vals[-2] if len(valid_vals) > 1 else curr
                    change = ((curr - prev) / prev) * 100 if prev != 0 else 0
                    
                    results[key] = {
                        "name": info["name"],
                        "ticker": info["ticker"],
                        "price": curr,
                        "change": change, # Percent change of the yield itself
                        "currency": "%"
                    }
                    
                    if key == "BE_10Y": be_val = curr
                    if key == "DE_10Y": de_val = curr
        except Exception as e:
            print(f"Error fetching {key}: {e}")

    # 3. Calculate Spread
    if be_val is not None and de_val is not None:
        spread = (be_val - de_val)
        # Spread change? Hard to calculate exact change without history, assume 0 or calc from yield changes?
        # Let's just calculate spread value. Change can be 0 for now or derived if I kept history.
        # Simple: Spread value.
        results["SPREAD"] = {
            "name": "BE-DE Spread",
            "ticker": "SPREAD",
            "price": spread,
            "change": 0, # Placeholder
            "currency": "%"
        }

    # Save
    with open("data/stocks.json", "w") as f:
        json.dump(results, f, indent=4)
    print("Successfully updated data/stocks.json")

if __name__ == "__main__":
    fetch_data()
