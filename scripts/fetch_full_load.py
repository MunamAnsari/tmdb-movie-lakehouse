import requests, time, json
from datetime import datetime, timezone

API_KEY = os.environ["TMDB_API_KEY"]
BASE_URL = "https://api.themoviedb.org/3"

def get_movie_details(movie_id):
    r = requests.get(f"{BASE_URL}/movie/{movie_id}", params={
        "api_key": API_KEY, "append_to_response": "credits,keywords,reviews"
    })
    r.raise_for_status()
    return r.json()

all_movies = []
for page in range(1, 11):  # 25 pages = ~500 movies
    r = requests.get(f"{BASE_URL}/discover/movie", params={
        "api_key": API_KEY, "sort_by": "popularity.desc", "page": page
    })
    for m in r.json().get("results", []):
        try:
            all_movies.append(get_movie_details(m["id"]))
        except Exception as e:
            print("skip", m["id"], e)
        time.sleep(0.05)
    print(f"page {page} done, total so far: {len(all_movies)}")

ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
fname = f"movies_full_load_{ts}.json"
with open(fname, "w") as f:
    json.dump({"load_type": "full", "record_count": len(all_movies), "records": all_movies}, f, indent=2)
print("saved", fname)
