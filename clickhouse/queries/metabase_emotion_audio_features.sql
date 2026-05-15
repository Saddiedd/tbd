SELECT
    emotion,
    count() AS track_count,
    round(avg(energy), 2) AS avg_energy,
    round(avg(danceability), 2) AS avg_danceability,
    round(avg(positiveness), 2) AS avg_positiveness,
    round(avg(acousticness), 2) AS avg_acousticness
FROM spotify_emotion_popularity_mt
WHERE emotion IS NOT NULL AND emotion != ''
GROUP BY emotion
ORDER BY track_count DESC;
