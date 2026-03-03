import requests

urls = {
    "BE_10Y": "https://api.db.nomics.world/v22/series/Eurostat/irt_lt_mcby_d/D.MCBY.BE?observations=true",
    "DE_10Y": "https://api.db.nomics.world/v22/series/Eurostat/irt_lt_mcby_d/D.MCBY.DE?observations=true",
    "ECB_MRO": "https://api.db.nomics.world/v22/series/ECB/FM/D.U2.EUR.4F.KR.MRR_FR.LEV?observations=true",
    "ECB_DEP": "https://api.db.nomics.world/v22/series/ECB/FM/D.U2.EUR.4F.KR.DFR.LEV?observations=true"
}

for name, url in urls.items():
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            series = data["series"]["docs"][0]
            last_date = series["period"][-1]
            last_val = series["value"][-1]
            print(f"{name}: {last_val} (at {last_date})")
        else:
            print(f"{name}: Failed {r.status_code}")
    except Exception as e:
        print(f"{name}: Error {e}")
