import mysql.connector


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "music_bp1_dwh",
}


def drop_marts(cur):
    cur.execute("DROP TABLE IF EXISTS mart_popularity_by_genre")
    cur.execute("DROP TABLE IF EXISTS mart_popularity_by_emotion")


def create_mart_popularity_by_genre(cur):
    cur.execute(
        """
        CREATE TABLE mart_popularity_by_genre AS
        SELECT
            g.genre_id,
            g.genre_name,
            COUNT(*) AS track_count,
            ROUND(AVG(f.popularity), 2) AS avg_popularity,
            ROUND(AVG(f.energy), 2) AS avg_energy,
            ROUND(AVG(f.danceability), 2) AS avg_danceability,
            ROUND(AVG(f.positiveness), 2) AS avg_positiveness,
            ROUND(AVG(f.speechiness), 2) AS avg_speechiness,
            ROUND(AVG(f.liveness), 2) AS avg_liveness,
            ROUND(AVG(f.acousticness), 2) AS avg_acousticness,
            ROUND(AVG(f.instrumentalness), 2) AS avg_instrumentalness,
            ROUND(AVG(f.tempo), 2) AS avg_tempo,
            ROUND(AVG(f.loudness_db), 2) AS avg_loudness_db
        FROM fact_track_popularity f
        JOIN dim_genres g ON g.genre_id = f.genre_id
        GROUP BY g.genre_id, g.genre_name
        """
    )

    cur.execute(
        """
        ALTER TABLE mart_popularity_by_genre
        MODIFY genre_id BIGINT UNSIGNED NOT NULL,
        MODIFY genre_name VARCHAR(760) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
        ADD PRIMARY KEY (genre_id)
        """
    )


def create_mart_popularity_by_emotion(cur):
    cur.execute(
        """
        CREATE TABLE mart_popularity_by_emotion AS
        SELECT
            e.emotion_id,
            e.emotion_name,
            COUNT(*) AS track_count,
            ROUND(AVG(f.popularity), 2) AS avg_popularity,
            ROUND(AVG(f.energy), 2) AS avg_energy,
            ROUND(AVG(f.danceability), 2) AS avg_danceability,
            ROUND(AVG(f.positiveness), 2) AS avg_positiveness,
            ROUND(AVG(f.speechiness), 2) AS avg_speechiness,
            ROUND(AVG(f.liveness), 2) AS avg_liveness,
            ROUND(AVG(f.acousticness), 2) AS avg_acousticness,
            ROUND(AVG(f.instrumentalness), 2) AS avg_instrumentalness,
            ROUND(AVG(f.tempo), 2) AS avg_tempo,
            ROUND(AVG(f.loudness_db), 2) AS avg_loudness_db
        FROM fact_track_popularity f
        JOIN dim_emotions e ON e.emotion_id = f.emotion_id
        GROUP BY e.emotion_id, e.emotion_name
        """
    )

    cur.execute(
        """
        ALTER TABLE mart_popularity_by_emotion
        MODIFY emotion_id BIGINT UNSIGNED NOT NULL,
        MODIFY emotion_name VARCHAR(760) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
        ADD PRIMARY KEY (emotion_id)
        """
    )


def main():
    conn = None
    cur = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        drop_marts(cur)
        create_mart_popularity_by_genre(cur)
        create_mart_popularity_by_emotion(cur)
        conn.commit()

        print("Data marts created successfully")

    except Exception as e:
        print(f"MySQL error: {e}")
        if conn is not None and conn.is_connected():
            try:
                conn.rollback()
            except Exception as rollback_error:
                print(f"Rollback failed: {rollback_error}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
