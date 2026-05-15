import time

import requests

URL = "http://localhost:8123"

queries = {
    "genre": """
SELECT genre, round(avg(popularity), 2) AS avg_popularity
FROM spotify_genre_popularity_mt
GROUP BY genre
ORDER BY avg_popularity DESC
FORMAT JSON
""",
    "emotion": """
SELECT emotion, round(avg(popularity), 2) AS avg_popularity
FROM spotify_emotion_popularity_mt
GROUP BY emotion
ORDER BY avg_popularity DESC
FORMAT JSON
""",
}

for name, sql in queries.items():
    start = time.perf_counter()
    response = requests.post(URL, data=sql.encode("utf-8"), timeout=60)
    response.raise_for_status()
    elapsed = time.perf_counter() - start
    print(f"ClickHouse {name} query time: {elapsed:.6f}s")
