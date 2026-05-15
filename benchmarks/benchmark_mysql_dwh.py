import os
import time

import pymysql

conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST", "localhost"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DB", "music_bp1_dwh"),
)

queries = {
    "genre": "SELECT genre_name, avg_popularity FROM mart_popularity_by_genre ORDER BY avg_popularity DESC",
    "emotion": "SELECT emotion_name, avg_popularity FROM mart_popularity_by_emotion ORDER BY avg_popularity DESC",
}

with conn.cursor() as cur:
    for name, sql in queries.items():
        start = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        elapsed = time.perf_counter() - start
        print(f"MySQL {name} query time: {elapsed:.6f}s")

conn.close()
