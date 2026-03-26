import requests
import pandas as pd
import time
import datetime as dt
from tqdm import tqdm

FLICKR_API_KEY = "078326a76eb8f23d718d7e00c47f24c7"

SEARCH_KEYWORDS = [
    "La Biennale di Venezia",
    "Biennale Architettura",
    "Biennale Teatro Venice",
    "Biennale Danza Venice",
    "Biennale Musica Venice",
    "Venice Glass Week",
    "Venice Photography Exhibition",
    "Venice Fashion Week",
    "Carnevale di Venezia",
    "Festa del Redentore",
    "Festa della Sensa",
    "Blessing of the Gondolas",
    "Vogalonga Venice",
    "Venice Marathon",
    "Open Studio Venice",
    "Rialto Market Venice",
    "Christmas in Venice",
    "Feast of St. George Venice",
    "Open Arsenale Venice",
    "Giants of the Lagoon Venice",
    "Venice Food Festival",
    "Venice Residencies",
    "Fondazione Venezia cultural events",
    "Acqua Alta Venice",
    "MOSE Venice open day",
    "Regata Storica Venice"
]

START_YEAR = 2000
END_YEAR = 2024
MAX_PER_QUERY = 200000

def date_to_unix(y, m, d):
    return int(time.mktime(dt.datetime(y, m, d, 0, 0, 0).timetuple()))

def fetch_flickr(year, keyword, max_limit=2000):
    url = "https://api.flickr.com/services/rest/"

    # 用 “YYYY-MM-DD” 这种字符串，而不是 Unix 时间戳
    min_date = f"{year}-01-01"
    max_date = f"{year}-12-31"

    page = 1
    per_page = 250
    total_collected = 0
    results = []

    bar = tqdm(desc=f"{year} | {keyword}", unit="photo")

    while True:
        params = {
            "method": "flickr.photos.search",
            "api_key": FLICKR_API_KEY,
            "text": keyword,
            "has_geo": 1,   # 只要有经纬度的
            "extras": "geo,date_taken,date_upload,owner_name,tags,url_m",
            "min_taken_date": min_date,   # 改成字符串
            "max_taken_date": max_date,
            "format": "json",
            "media": "photos",
            "nojsoncallback": 1,
            "per_page": per_page,
            "page": page,
        }

        resp = requests.get(url, params=params)
        data = resp.json()

        # ★ 调试：第一次请求的时候打印一下 total 看看 Flickr 那边到底有没有图
        if page == 1 and total_collected == 0:
            print(f"\nDEBUG {year} | {keyword} → total =", data.get("photos", {}).get("total"))

        if data.get("stat") == "fail":
            print("❌ API 错误：", data)
            break

        photos = data["photos"]["photo"]
        if not photos:
            break

        for p in photos:
            lat=p.get("latitude")
            lon=p.get("longitude")

            if  not lat or not lon:
                continue
            results.append({
                "event": keyword,
                "year": year,
                "photo_id": p.get("id"),
                "lat": float(lat),
                "lon": float(lon),
                "datetaken": p.get("datetaken"),
                "ownername": p.get("ownername"),
                "tags": p.get("tags"),
                "url_m": p.get("url_m"),
            })
            total_collected += 1
            bar.update(1)

            if total_collected >= max_limit:
                bar.close()
                return results

        if page >= data["photos"]["pages"]:
            break

        page += 1
        time.sleep(0.2)

    bar.close()
    return results


all_data = []

for year in range(START_YEAR, END_YEAR + 1):
    for kw in SEARCH_KEYWORDS:
        rows = fetch_flickr(year, kw, MAX_PER_QUERY)
        all_data.extend(rows)

df = pd.DataFrame(all_data)
df.to_csv("venice_cultural_events_with_geo.csv", index=False, encoding="utf-8-sig")

print("✔ 完成！总共抓到照片：", len(df))
