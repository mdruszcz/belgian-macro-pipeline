import urllib.request
import json
import time

stock_list = [
    {"id": "BEL20", "ticker": "^BFX", "name": "BEL20 Index"},
    {"id": "EURUSD", "ticker": "EURUSD=X", "name": "Euro / US Dollar"},
    {"id": "ABI", "ticker": "ABI.BR", "name": "AB InBev"},
    {"id": "ACKB", "ticker": "ACKB.BR", "name": "Ackermans & van Haaren"},
    {"id": "AED", "ticker": "AED.BR", "name": "Aedifica"},
    {"id": "AGS", "ticker": "AGS.BR", "name": "Ageas"},
    {"id": "ARGX", "ticker": "ARGX.BR", "name": "Argenx"},
    {"id": "AZE", "ticker": "AZE.BR", "name": "Azelis Group"},
    {"id": "COFB", "ticker": "COFB.BR", "name": "Cofinimmo"},
    {"id": "DIE", "ticker": "DIE.BR", "name": "D'Ieteren Group"},
    {"id": "ELI", "ticker": "ELI.BR", "name": "Elia Group"},
    {"id": "GLPG", "ticker": "GLPG.BR", "name": "Galapagos"},
    {"id": "GBLB", "ticker": "GBLB.BR", "name": "Groupe Bruxelles Lambert"},
    {"id": "KBC", "ticker": "KBC.BR", "name": "KBC Group"},
    {"id": "LOTB", "ticker": "LOTB.BR", "name": "Lotus Bakeries"},
    {"id": "MELE", "ticker": "MELE.BR", "name": "Melexis"},
    {"id": "SOF", "ticker": "SOF.BR", "name": "Sofina"},
    {"id": "SOLB", "ticker": "SOLB.BR", "name": "Solvay"},
    {"id": "SYENS", "ticker": "SYENS.BR", "name": "Syensqo"},
    {"id": "UCB", "ticker": "UCB.BR", "name": "UCB"},
    {"id": "UMI", "ticker": "UMI.BR", "name": "Umicore"},
    {"id": "WDP", "ticker": "WDP.BR", "name": "WDP"}
]

def fetch_data():
    results = {}
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Try to batch symbols using the quote API first as it's more efficient
    tickers_str = ",".join([s["ticker"] for s in stock_list])
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={tickers_str}"
    
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            if "quoteResponse" in data and "result" in data["quoteResponse"]:
                for quote in data["quoteResponse"]["result"]:
                    ticker = quote["symbol"]
                    stock_info = next((s for s in stock_list if s["ticker"] == ticker), None)
                    if stock_info:
                        price = quote.get("regularMarketPrice", 0)
                        change = quote.get("regularMarketChangePercent", 0)
                        currency = quote.get("currency", "EUR")
                        
                        results[stock_info["id"]] = {
                            "name": stock_info["name"],
                            "ticker": stock_info["id"],
                            "price": price,
                            "change": change,
                            "currency": currency
                        }
    except Exception as e:
        print(f"Error fetching batch data: {e}")

    # For any missing data, try individual chart requests (more robust)
    for stock in stock_list:
        if stock["id"] not in results:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock['ticker']}?interval=1d&range=5d"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode())
                    meta = data["chart"]["result"][0]["meta"]
                    price = meta.get("regularMarketPrice")
                    # Fallback for previous close
                    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
                    
                    if price is None and "indicators" in data["chart"]["result"][0]:
                        adjclose = data["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]
                        price = adjclose[-1]
                        if prev_close is None and len(adjclose) > 1:
                            prev_close = adjclose[-2]

                    if price is not None and prev_close:
                        change = ((price - prev_close) / prev_close) * 100
                        results[stock["id"]] = {
                            "name": stock["name"],
                            "ticker": stock["id"],
                            "price": price,
                            "change": change,
                            "currency": meta.get("currency", "EUR")
                        }
                time.sleep(0.5) # Be polite
            except Exception as e:
                print(f"Error fetching {stock['id']}: {e}")

    with open("data/stocks.json", "w") as f:
        json.dump(results, f, indent=4)
    print("Successfully updated data/stocks.json")

if __name__ == "__main__":
    fetch_data()
