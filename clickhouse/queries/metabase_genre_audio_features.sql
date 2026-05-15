SELECT
    genre,
    count() AS track_count,
    round(avg(energy), 2) AS avg_energy,
    round(avg(danceability), 2) AS avg_danceability,
    round(avg(positiveness), 2) AS avg_positiveness,
    round(avg(acousticness), 2) AS avg_acousticness
FROM spotify_genre_popularity_mt
WHERE genre IS NOT NULL AND genre != ''
GROUP BY genre
HAVING track_count >= 5
ORDER BY track_count DESC
LIMIT 15;
