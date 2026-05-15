SELECT
    genre,
    count() AS track_count,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_genre_popularity_mt
WHERE genre IS NOT NULL AND genre != ''
GROUP BY genre
HAVING track_count >= 5
ORDER BY avg_popularity DESC
LIMIT 20;
