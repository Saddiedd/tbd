import csv
import math
import mysql.connector


CSV_PATH = "spotify_dataset.csv"
TABLE_NAME = "fact_tracks_denorm"
BATCH_SIZE = 100
TRUNCATE_BEFORE_LOAD = False


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "music",
}


GUIDE_TABLES = [
    {
        "name": "artists",
        "guide_table": "guide_artists",
        "guide_pk": "artist_id",
        "guide_value": "artist_name",
        "csv_columns": ["Artist(s)", "Similar Artist 1", "Similar Artist 2", "Similar Artist 3"],
    },
    {
        "name": "songs",
        "guide_table": "guide_songs",
        "guide_pk": "song_id",
        "guide_value": "song_name",
        "csv_columns": ["song", "Similar Song 1", "Similar Song 2", "Similar Song 3"],
    },
    {
        "name": "emotions",
        "guide_table": "guide_emotions",
        "guide_pk": "emotion_id",
        "guide_value": "emotion_name",
        "csv_columns": ["emotion"],
    },
    {
        "name": "genres",
        "guide_table": "guide_genres",
        "guide_pk": "genre_id",
        "guide_value": "genre_name",
        "csv_columns": ["Genre"],
    },
    {
        "name": "albums",
        "guide_table": "guide_albums",
        "guide_pk": "album_id",
        "guide_value": "album_name",
        "csv_columns": ["Album"],
    },
    {
        "name": "release_dates",
        "guide_table": "guide_release_dates",
        "guide_pk": "release_date_id",
        "guide_value": "release_date_text",
        "csv_columns": ["Release Date"],
    },
    {
        "name": "musical_keys",
        "guide_table": "guide_musical_keys",
        "guide_pk": "musical_key_id",
        "guide_value": "musical_key",
        "csv_columns": ["Key"],
    },
    {
        "name": "time_signatures",
        "guide_table": "guide_time_signatures",
        "guide_pk": "time_signature_id",
        "guide_value": "time_signature",
        "csv_columns": ["Time signature"],
    },
]


FACT_COLUMNS = [
    ("Artist(s)", "artist_id", "artists"),
    ("song", "song_id", "songs"),
    ("Length", "track_length_seconds", "length_seconds"),
    ("emotion", "emotion_id", "emotions"),
    ("Genre", "genre_id", "genres"),
    ("Album", "album_id", "albums"),
    ("Release Date", "release_date_id", "release_dates"),
    ("Key", "musical_key_id", "musical_keys"),
    ("Tempo", "tempo", "int"),
    ("Loudness (db)", "loudness_db", "loudness"),
    ("Time signature", "time_signature_id", "time_signatures"),
    ("Explicit", "explicit_flag", "explicit_flag"),
    ("Popularity", "popularity", "int"),
    ("Energy", "energy", "float"),
    ("Danceability", "danceability", "float"),
    ("Positiveness", "positiveness", "float"),
    ("Speechiness", "speechiness", "float"),
    ("Liveness", "liveness", "float"),
    ("Acousticness", "acousticness", "float"),
    ("Instrumentalness", "instrumentalness", "float"),
    ("Good for Party", "good_for_party", "int"),
    ("Good for Work/Study", "good_for_work_study", "int"),
    ("Good for Relaxation/Meditation", "good_for_relaxation_meditation", "int"),
    ("Good for Exercise", "good_for_exercise", "int"),
    ("Good for Running", "good_for_running", "int"),
    ("Good for Yoga/Stretching", "good_for_yoga_stretching", "int"),
    ("Good for Driving", "good_for_driving", "int"),
    ("Good for Social Gatherings", "good_for_social_gatherings", "int"),
    ("Good for Morning Routine", "good_for_morning_routine", "int"),
    ("Similar Artist 1", "similar_artist_1_id", "artists"),
    ("Similar Song 1", "similar_song_1_id", "songs"),
    ("Similarity Score 1", "similarity_score_1", "float"),
    ("Similar Artist 2", "similar_artist_2_id", "artists"),
    ("Similar Song 2", "similar_song_2_id", "songs"),
    ("Similarity Score 2", "similarity_score_2", "float"),
    ("Similar Artist 3", "similar_artist_3_id", "artists"),
    ("Similar Song 3", "similar_song_3_id", "songs"),
    ("Similarity Score 3", "similarity_score_3", "float"),
]


def to_int(value: str):
    if value is None:
        return None
    val = value.strip()
    if val == "" or val.lower() in {"nan", "null", "none"}:
        return None
    try:
        number = float(val)
    except Exception as e:
        raise ValueError(f"Int error: {value!r}") from e
    if math.isnan(number) or math.isinf(number):
        return None
    if not number.is_integer():
        raise ValueError(f"Expected integer value, got {value!r}")
    return int(number)


def to_float(value: str):
    if value is None:
        return None
    val = value.strip()
    if val == "" or val.lower() in {"nan", "null", "none"}:
        return None
    try:
        number = float(val)
    except Exception as e:
        raise ValueError(f"Float error: {value!r}") from e
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def to_loudness(value: str):
    if value is None:
        return None
    val = value.strip().lower().replace("db", "")
    if val == "":
        return None
    try:
        return float(val)
    except Exception as e:
        raise ValueError(f"Float error (loudness): {value!r}") from e


def to_explicit_flag(value: str):
    if value is None:
        return None
    val = value.strip().lower()
    if val in ("yes", "true", "1"):
        return 1
    if val in ("no", "false", "0"):
        return 0
    return None


def to_text(value: str):
    if value is None:
        return None
    val = value.strip()
    return val if val else None


def to_length_seconds(value: str):
    text_value = to_text(value)
    if text_value is None:
        return None

    parts = text_value.split(":")
    try:
        if len(parts) == 2:
            minutes, seconds = parts
            return int(minutes) * 60 + int(seconds)
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    except ValueError as e:
        raise ValueError(f"Length parse error: {value!r}") from e

    raise ValueError(f"Unsupported length format: {value!r}")


def open_csv_dict_reader():
    csv_file = open(CSV_PATH, "r", encoding="utf-8-sig", errors="replace", newline="")
    sample = csv_file.read(8192)
    csv_file.seek(0)

    try:
        sniffed = csv.Sniffer().sniff(sample, delimiters=",;")
        delimiter = sniffed.delimiter
    except csv.Error:
        delimiter = ","

    return csv_file, csv.DictReader(csv_file, delimiter=delimiter, quotechar='"')


def create_guide_tables(cur):
    for config in GUIDE_TABLES:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config["guide_table"]} (
                {config["guide_pk"]} BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                {config["guide_value"]} VARCHAR(760) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                UNIQUE KEY uq_{config["guide_table"]}_value ({config["guide_value"]})
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def drop_existing_tables(cur):
    cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    for config in GUIDE_TABLES:
        cur.execute(f"DROP TABLE IF EXISTS {config['guide_table']}")


def create_table(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            track_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            artist_id BIGINT UNSIGNED,
            song_id BIGINT UNSIGNED,
            track_length_seconds INT,
            emotion_id BIGINT UNSIGNED,
            genre_id BIGINT UNSIGNED,
            album_id BIGINT UNSIGNED,
            release_date_id BIGINT UNSIGNED,
            musical_key_id BIGINT UNSIGNED,
            tempo INT,
            loudness_db DECIMAL(7,3),
            time_signature_id BIGINT UNSIGNED,
            explicit_flag TINYINT(1),
            popularity INT,
            energy DOUBLE,
            danceability DOUBLE,
            positiveness DOUBLE,
            speechiness DOUBLE,
            liveness DOUBLE,
            acousticness DOUBLE,
            instrumentalness DOUBLE,
            good_for_party INT,
            good_for_work_study INT,
            good_for_relaxation_meditation INT,
            good_for_exercise INT,
            good_for_running INT,
            good_for_yoga_stretching INT,
            good_for_driving INT,
            good_for_social_gatherings INT,
            good_for_morning_routine INT,
            similar_artist_1_id BIGINT UNSIGNED,
            similar_song_1_id BIGINT UNSIGNED,
            similarity_score_1 DOUBLE,
            similar_artist_2_id BIGINT UNSIGNED,
            similar_song_2_id BIGINT UNSIGNED,
            similarity_score_2 DOUBLE,
            similar_artist_3_id BIGINT UNSIGNED,
            similar_song_3_id BIGINT UNSIGNED,
            similarity_score_3 DOUBLE,
            INDEX idx_artist_id (artist_id),
            INDEX idx_song_id (song_id),
            INDEX idx_emotion_id (emotion_id),
            INDEX idx_genre_id (genre_id),
            INDEX idx_album_id (album_id),
            INDEX idx_release_date_id (release_date_id),
            INDEX idx_musical_key_id (musical_key_id),
            INDEX idx_time_signature_id (time_signature_id),
            INDEX idx_similar_artist_1_id (similar_artist_1_id),
            INDEX idx_similar_song_1_id (similar_song_1_id),
            INDEX idx_similar_artist_2_id (similar_artist_2_id),
            INDEX idx_similar_song_2_id (similar_song_2_id),
            INDEX idx_similar_artist_3_id (similar_artist_3_id),
            INDEX idx_similar_song_3_id (similar_song_3_id),
            CONSTRAINT fk_fact_artist
                FOREIGN KEY (artist_id) REFERENCES guide_artists(artist_id),
            CONSTRAINT fk_fact_song
                FOREIGN KEY (song_id) REFERENCES guide_songs(song_id),
            CONSTRAINT fk_fact_emotion
                FOREIGN KEY (emotion_id) REFERENCES guide_emotions(emotion_id),
            CONSTRAINT fk_fact_genre
                FOREIGN KEY (genre_id) REFERENCES guide_genres(genre_id),
            CONSTRAINT fk_fact_album
                FOREIGN KEY (album_id) REFERENCES guide_albums(album_id),
            CONSTRAINT fk_fact_release_date
                FOREIGN KEY (release_date_id) REFERENCES guide_release_dates(release_date_id),
            CONSTRAINT fk_fact_musical_key
                FOREIGN KEY (musical_key_id) REFERENCES guide_musical_keys(musical_key_id),
            CONSTRAINT fk_fact_time_signature
                FOREIGN KEY (time_signature_id) REFERENCES guide_time_signatures(time_signature_id),
            CONSTRAINT fk_fact_similar_artist_1
                FOREIGN KEY (similar_artist_1_id) REFERENCES guide_artists(artist_id),
            CONSTRAINT fk_fact_similar_song_1
                FOREIGN KEY (similar_song_1_id) REFERENCES guide_songs(song_id),
            CONSTRAINT fk_fact_similar_artist_2
                FOREIGN KEY (similar_artist_2_id) REFERENCES guide_artists(artist_id),
            CONSTRAINT fk_fact_similar_song_2
                FOREIGN KEY (similar_song_2_id) REFERENCES guide_songs(song_id),
            CONSTRAINT fk_fact_similar_artist_3
                FOREIGN KEY (similar_artist_3_id) REFERENCES guide_artists(artist_id),
            CONSTRAINT fk_fact_similar_song_3
                FOREIGN KEY (similar_song_3_id) REFERENCES guide_songs(song_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def collect_guide_values():
    values_by_guide = {config["name"]: set() for config in GUIDE_TABLES}

    csv_file, reader = open_csv_dict_reader()
    with csv_file:

        for record in reader:
            for config in GUIDE_TABLES:
                for csv_column in config["csv_columns"]:
                    value = to_text(record.get(csv_column))
                    if value is not None:
                        values_by_guide[config["name"]].add(value)

    return values_by_guide


def clear_guides(cur):
    for config in GUIDE_TABLES:
        cur.execute(f"DELETE FROM {config['guide_table']}")


def fill_guides(cur):
    values_by_guide = collect_guide_values()
    clear_guides(cur)

    for config in GUIDE_TABLES:
        values = sorted(values_by_guide[config["name"]])
        if not values:
            continue

        insert_sql = f"""
            INSERT INTO {config["guide_table"]} ({config["guide_value"]})
            VALUES (%s)
            """

        for start in range(0, len(values), BATCH_SIZE):
            batch_values = values[start : start + BATCH_SIZE]
            rows = [(value,) for value in batch_values]
            cur.executemany(insert_sql, rows)


def load_guide_maps(cur):
    guide_maps = {}

    for config in GUIDE_TABLES:
        cur.execute(
            f"""
            SELECT {config["guide_value"]}, {config["guide_pk"]}
            FROM {config["guide_table"]}
            """
        )
        guide_maps[config["name"]] = {value: guide_id for value, guide_id in cur.fetchall()}

    return guide_maps


def convert_field(record, csv_column, converter_name, guide_maps):
    raw_value = record.get(csv_column)

    if converter_name == "int":
        return to_int(raw_value)
    if converter_name == "float":
        return to_float(raw_value)
    if converter_name == "loudness":
        return to_loudness(raw_value)
    if converter_name == "explicit_flag":
        return to_explicit_flag(raw_value)
    if converter_name == "length_seconds":
        return to_length_seconds(raw_value)

    text_value = to_text(raw_value)
    if text_value is None:
        return None

    guide_map = guide_maps[converter_name]
    try:
        return guide_map[text_value]
    except KeyError as e:
        raise ValueError(f"Guide value not found for {csv_column}: {text_value!r}") from e


def build_row(record: dict, guide_maps: dict):
    return tuple(
        convert_field(record, csv_column, converter_name, guide_maps)
        for csv_column, _, converter_name in FACT_COLUMNS
    )


def create_guide(cur, conn):
    try:
        fill_guides(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def load_data(cur, conn):
    db_cols = [db_column for _, db_column, _ in FACT_COLUMNS]
    placeholders = ", ".join(["%s"] * len(db_cols))
    db_col_names = ", ".join(db_cols)
    guide_maps = load_guide_maps(cur)

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} ({db_col_names})
    VALUES ({placeholders})
    """

    if TRUNCATE_BEFORE_LOAD:
        cur.execute(f"DELETE FROM {TABLE_NAME}")

    csv_file, reader = open_csv_dict_reader()
    with csv_file:

        batch = []
        inserted_rows = 0
        batch_start_row = 2

        for row_number, record in enumerate(reader, start=2):
            batch.append(build_row(record, guide_maps))

            if len(batch) >= BATCH_SIZE:
                try:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    inserted_rows += len(batch)
                    print(f"inserted: {inserted_rows}\n")
                    batch.clear()
                    batch_start_row = row_number + 1
                except Exception as e:
                    batch.clear()
                    raise ValueError(
                        f"Batch insert failed for rows {batch_start_row}-{row_number}"
                    ) from e

        if batch:
            final_row_number = batch_start_row + len(batch) - 1
            try:
                cur.executemany(insert_sql, batch)
                conn.commit()
                inserted_rows += len(batch)
                print(f"inserted: {inserted_rows}\n")
                batch.clear()
            except Exception as e:
                batch.clear()
                raise ValueError(
                    f"Batch insert failed for rows {batch_start_row}-{final_row_number}"
                ) from e


def main():
    conn = None
    cur = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        drop_existing_tables(cur)
        create_guide_tables(cur)
        create_table(cur)
        conn.commit()

        create_guide(cur, conn)
        load_data(cur, conn)

        print("Done")

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
