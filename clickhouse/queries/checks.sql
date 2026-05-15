SHOW TABLES;

SELECT count() AS rows_in_genre_mt
FROM spotify_genre_popularity_mt;

SELECT count() AS rows_in_emotion_mt
FROM spotify_emotion_popularity_mt;

SELECT *
FROM spotify_genre_popularity_mt
LIMIT 10;

SELECT *
FROM spotify_emotion_popularity_mt
LIMIT 10;

SELECT
    min(loaded_at) AS first_loaded_at,
    max(loaded_at) AS last_loaded_at,
    count() AS rows_count
FROM spotify_genre_popularity_mt;
