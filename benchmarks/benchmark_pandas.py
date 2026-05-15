import time
from pathlib import Path

import pandas as pd

DATASET_CANDIDATES = [Path("../data/spotify_dataset_clear.csv"), Path("../data/spotify_dataset.csv")]

def detect_column(columns, variants):
    for v in variants:
        if v in columns:
            return v
    raise KeyError(f"None of {variants} found")


dataset_path = next((p for p in DATASET_CANDIDATES if p.exists()), None)
if dataset_path is None:
    raise FileNotFoundError("No full dataset found in ../data/")

df = pd.read_csv(dataset_path)

genre_col = detect_column(df.columns, ["Genre", "genre"])
emotion_col = detect_column(df.columns, ["emotion", "Emotion"])
popularity_col = detect_column(df.columns, ["Popularity", "popularity"])

start = time.perf_counter()
_ = df.groupby(genre_col)[popularity_col].mean().reset_index().sort_values(popularity_col, ascending=False)
genre_time = time.perf_counter() - start

start = time.perf_counter()
_ = df.groupby(emotion_col)[popularity_col].mean().reset_index().sort_values(popularity_col, ascending=False)
emotion_time = time.perf_counter() - start

print(f"Pandas genre query time: {genre_time:.6f}s")
print(f"Pandas emotion query time: {emotion_time:.6f}s")
