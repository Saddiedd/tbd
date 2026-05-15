SELECT
    genre,
    count() AS track_count,
    round(avg(popularity), 2) AS avg_popularity,
    round(avg(energy), 2) AS avg_energy,
    round(avg(danceability), 2) AS avg_danceability,
    round(avg(positiveness), 2) AS avg_positiveness,
    round(avg(speechiness), 2) AS avg_speechiness,
    round(avg(liveness), 2) AS avg_liveness,
    round(avg(acousticness), 2) AS avg_acousticness,
    round(avg(instrumentalness), 2) AS avg_instrumentalness,
    round(avg(tempo), 2) AS avg_tempo,
    round(avg(loudness_db), 2) AS avg_loudness_db
FROM spotify_genre_popularity_mt
GROUP BY genre
ORDER BY avg_popularity DESC;

SELECT
    emotion,
    count() AS track_count,
    round(avg(popularity), 2) AS avg_popularity,
    round(avg(energy), 2) AS avg_energy,
    round(avg(danceability), 2) AS avg_danceability,
    round(avg(positiveness), 2) AS avg_positiveness,
    round(avg(speechiness), 2) AS avg_speechiness,
    round(avg(liveness), 2) AS avg_liveness,
    round(avg(acousticness), 2) AS avg_acousticness,
    round(avg(instrumentalness), 2) AS avg_instrumentalness,
    round(avg(tempo), 2) AS avg_tempo,
    round(avg(loudness_db), 2) AS avg_loudness_db
FROM spotify_emotion_popularity_mt
GROUP BY emotion
ORDER BY avg_popularity DESC;
