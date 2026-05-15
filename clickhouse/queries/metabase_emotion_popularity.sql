SELECT
    emotion,
    count() AS track_count,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_emotion_popularity_mt
WHERE emotion IS NOT NULL AND emotion != ''
GROUP BY emotion
ORDER BY avg_popularity DESC;
