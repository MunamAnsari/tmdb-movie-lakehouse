import os, requests, time, json
from datetime import datetime, timedelta, timezone

API_KEY = os.environ["TMDB_API_KEY"]
BASE_URL = "https://api.themoviedb.org/3"

def get_movie_details(movie_id):
    r = requests.get(f"{BASE_URL}/movie/{movie_id}", params={
        "api_key": API_KEY, "append_to_response": "credits,keywords,reviews"
    })
    r.raise_for_status()
    return r.json()

# TMDB allows up to 14 days back — widen this if yesterday's window is too thin
today = datetime.now(timezone.utc).date()
start_date = (today - timedelta(days=7)).isoformat()
end_date = today.isoformat()

changed_ids = []
page = 1
while True:
    r = requests.get(f"{BASE_URL}/movie/changes", params={
        "api_key": API_KEY, "start_date": start_date, "end_date": end_date, "page": page
    })
    data = r.json()
    changed_ids.extend([item["id"] for item in data.get("results", [])])
    if page >= data.get("total_pages", 1):
        break
    page += 1
    time.sleep(0.2)

print(f"Found {len(changed_ids)} changed movie IDs between {start_date} and {end_date}")

changed_ids = changed_ids[:300]  # cap so the sample stays small

changed_movies = []
for i, mid in enumerate(changed_ids, 1):
    try:
        changed_movies.append(get_movie_details(mid))
    except Exception as e:
        print("skip", mid, e)
    time.sleep(0.05)
    if i % 50 == 0:
        print(f"{i}/{len(changed_ids)} fetched")

ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
fname = f"movies_incremental_load_{ts}.json"
with open(fname, "w") as f:
    json.dump({
        "load_type": "incremental",
        "window_start": start_date,
        "window_end": end_date,
        "record_count": len(changed_movies),
        "records": changed_movies
    }, f, indent=2)

print("saved", fname)
