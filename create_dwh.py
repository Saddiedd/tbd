import mysql.connector


SOURCE_DB = "music"
TARGET_DB = "music_bp1_dwh"


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
}


DIMENSION_TABLES = [
    {
        "source_table": "guide_artists",
        "target_table": "dim_artists",
        "id_column": "artist_id",
        "value_column": "artist_name",
    },
    {
        "source_table": "guide_emotions",
        "target_table": "dim_emotions",
        "id_column": "emotion_id",
        "value_column": "emotion_name",
    },
    {
        "source_table": "guide_genres",
        "target_table": "dim_genres",
        "id_column": "genre_id",
        "value_column": "genre_name",
    },
    {
        "source_table": "guide_albums",
        "target_table": "dim_albums",
        "id_column": "album_id",
        "value_column": "album_name",
    },
]


def recreate_database(cur):
    cur.execute(f"DROP DATABASE IF EXISTS {TARGET_DB}")
    cur.execute(f"CREATE DATABASE {TARGET_DB} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")


def create_dimensions(cur):
    for config in DIMENSION_TABLES:
        cur.execute(
            f"""
            CREATE TABLE {TARGET_DB}.{config["target_table"]} AS
            SELECT
                {config["id_column"]},
                {config["value_column"]}
            FROM {SOURCE_DB}.{config["source_table"]}
            """
        )

        cur.execute(
            f"""
            ALTER TABLE {TARGET_DB}.{config["target_table"]}
            MODIFY {config["id_column"]} BIGINT UNSIGNED NOT NULL,
            MODIFY {config["value_column"]} VARCHAR(760) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
            ADD PRIMARY KEY ({config["id_column"]})
            """
        )


def create_fact_table(cur):
    cur.execute(
        f"""
        CREATE TABLE {TARGET_DB}.fact_track_popularity (
            popularity_fact_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            source_track_id BIGINT UNSIGNED NOT NULL,
            artist_id BIGINT UNSIGNED,
            emotion_id BIGINT UNSIGNED,
            genre_id BIGINT UNSIGNED,
            album_id BIGINT UNSIGNED,
            track_length_seconds INT,
            explicit_flag TINYINT(1),
            popularity INT,
            tempo INT,
            loudness_db DECIMAL(7,3),
            energy DOUBLE,
            danceability DOUBLE,
            positiveness DOUBLE,
            speechiness DOUBLE,
            liveness DOUBLE,
            acousticness DOUBLE,
            instrumentalness DOUBLE,
            INDEX idx_artist_id (artist_id),
            INDEX idx_emotion_id (emotion_id),
            INDEX idx_genre_id (genre_id),
            INDEX idx_album_id (album_id),
            CONSTRAINT fk_dwh_artist
                FOREIGN KEY (artist_id) REFERENCES {TARGET_DB}.dim_artists(artist_id),
            CONSTRAINT fk_dwh_emotion
                FOREIGN KEY (emotion_id) REFERENCES {TARGET_DB}.dim_emotions(emotion_id),
            CONSTRAINT fk_dwh_genre
                FOREIGN KEY (genre_id) REFERENCES {TARGET_DB}.dim_genres(genre_id),
            CONSTRAINT fk_dwh_album
                FOREIGN KEY (album_id) REFERENCES {TARGET_DB}.dim_albums(album_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

    cur.execute(
        f"""
        INSERT INTO {TARGET_DB}.fact_track_popularity (
            source_track_id,
            artist_id,
            emotion_id,
            genre_id,
            album_id,
            track_length_seconds,
            explicit_flag,
            popularity,
            tempo,
            loudness_db,
            energy,
            danceability,
            positiveness,
            speechiness,
            liveness,
            acousticness,
            instrumentalness
        )
        SELECT
            track_id,
            artist_id,
            emotion_id,
            genre_id,
            album_id,
            track_length_seconds,
            explicit_flag,
            popularity,
            tempo,
            loudness_db,
            energy,
            danceability,
            positiveness,
            speechiness,
            liveness,
            acousticness,
            instrumentalness
        FROM {SOURCE_DB}.fact_tracks_denorm
        """
    )


def main():
    conn = None
    cur = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        recreate_database(cur)
        create_dimensions(cur)
        create_fact_table(cur)
        conn.commit()

        print(f"DWH created in database: {TARGET_DB}")

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
