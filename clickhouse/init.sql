CREATE TABLE IF NOT EXISTS spotify_tracks_kafka
(
    artist String,
    song String,
    emotion String,
    genre String,
    album String,
    release_date String,
    track_length_seconds Nullable(UInt32),
    tempo Nullable(Float64),
    loudness_db Nullable(Float64),
    explicit_flag Nullable(UInt8),
    popularity Nullable(UInt8),
    energy Nullable(Float64),
    danceability Nullable(Float64),
    positiveness Nullable(Float64),
    speechiness Nullable(Float64),
    liveness Nullable(Float64),
    acousticness Nullable(Float64),
    instrumentalness Nullable(Float64),
    good_for_party Nullable(UInt8),
    good_for_work_study Nullable(UInt8),
    good_for_relaxation_meditation Nullable(UInt8),
    good_for_exercise Nullable(UInt8),
    good_for_running Nullable(UInt8),
    good_for_yoga_stretching Nullable(UInt8),
    good_for_driving Nullable(UInt8),
    good_for_social_gatherings Nullable(UInt8),
    good_for_morning_routine Nullable(UInt8)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'spotify_tracks_events',
    kafka_group_name = 'clickhouse_spotify_consumer_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS spotify_genre_popularity_mt
(
    genre LowCardinality(String),
    popularity Nullable(UInt8),
    energy Nullable(Float64),
    danceability Nullable(Float64),
    positiveness Nullable(Float64),
    speechiness Nullable(Float64),
    liveness Nullable(Float64),
    acousticness Nullable(Float64),
    instrumentalness Nullable(Float64),
    tempo Nullable(Float64),
    loudness_db Nullable(Float64),
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (genre, loaded_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS spotify_genre_popularity_mv
TO spotify_genre_popularity_mt
AS
SELECT
    genre,
    popularity,
    energy,
    danceability,
    positiveness,
    speechiness,
    liveness,
    acousticness,
    instrumentalness,
    tempo,
    loudness_db,
    now() AS loaded_at
FROM spotify_tracks_kafka;

CREATE TABLE IF NOT EXISTS spotify_emotion_popularity_mt
(
    emotion LowCardinality(String),
    popularity Nullable(UInt8),
    energy Nullable(Float64),
    danceability Nullable(Float64),
    positiveness Nullable(Float64),
    speechiness Nullable(Float64),
    liveness Nullable(Float64),
    acousticness Nullable(Float64),
    instrumentalness Nullable(Float64),
    tempo Nullable(Float64),
    loudness_db Nullable(Float64),
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (emotion, loaded_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS spotify_emotion_popularity_mv
TO spotify_emotion_popularity_mt
AS
SELECT
    emotion,
    popularity,
    energy,
    danceability,
    positiveness,
    speechiness,
    liveness,
    acousticness,
    instrumentalness,
    tempo,
    loudness_db,
    now() AS loaded_at
FROM spotify_tracks_kafka;
