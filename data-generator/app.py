import csv
import json
import logging
import os
import socket
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


FIELD_MAP = {
    "Artist(s)": "artist",
    "song": "song",
    "emotion": "emotion",
    "Genre": "genre",
    "Album": "album",
    "Release Date": "release_date",
    "Length": "track_length_seconds",
    "Tempo": "tempo",
    "Loudness (db)": "loudness_db",
    "Explicit": "explicit_flag",
    "Popularity": "popularity",
    "Energy": "energy",
    "Danceability": "danceability",
    "Positiveness": "positiveness",
    "Speechiness": "speechiness",
    "Liveness": "liveness",
    "Acousticness": "acousticness",
    "Instrumentalness": "instrumentalness",
    "Good for Party": "good_for_party",
    "Good for Work/Study": "good_for_work_study",
    "Good for Relaxation/Meditation": "good_for_relaxation_meditation",
    "Good for Exercise": "good_for_exercise",
    "Good for Running": "good_for_running",
    "Good for Yoga/Stretching": "good_for_yoga_stretching",
    "Good for Driving": "good_for_driving",
    "Good for Social Gatherings": "good_for_social_gatherings",
    "Good for Morning Routine": "good_for_morning_routine",
}

INT_FIELDS = {
    "track_length_seconds",
    "explicit_flag",
    "popularity",
    "good_for_party",
    "good_for_work_study",
    "good_for_relaxation_meditation",
    "good_for_exercise",
    "good_for_running",
    "good_for_yoga_stretching",
    "good_for_driving",
    "good_for_social_gatherings",
    "good_for_morning_routine",
}
FLOAT_FIELDS = {
    "tempo",
    "loudness_db",
    "energy",
    "danceability",
    "positiveness",
    "speechiness",
    "liveness",
    "acousticness",
    "instrumentalness",
}


def detect_delimiter(file_path: Path) -> str:
    sample = file_path.read_text(encoding="utf-8", errors="ignore")[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","


def parse_nullable_int(raw):
    if raw is None:
        return None
    value = str(raw).strip()
    if value == "":
        return None
    try:
        if ":" in value:
            parts = value.split(":")
            if len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
        return int(float(value))
    except ValueError:
        return None


def parse_nullable_float(raw):
    if raw is None:
        return None
    value = str(raw).strip().lower().replace("db", "").strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def normalize_row(row):
    result = {}
    for source_col, target_col in FIELD_MAP.items():
        raw_value = row.get(source_col)
        if target_col in INT_FIELDS:
            result[target_col] = parse_nullable_int(raw_value)
        elif target_col in FLOAT_FIELDS:
            result[target_col] = parse_nullable_float(raw_value)
        else:
            result[target_col] = "" if raw_value is None else str(raw_value).strip()
    return result


def check_kafka_connection(host="kafka", port=29092, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_for_kafka(max_attempts=60, delay_sec=2):
    broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    host, port = broker.split(":")
    port = int(port)
    logger.info("Waiting for Kafka at %s", broker)

    for attempt in range(1, max_attempts + 1):
        if check_kafka_connection(host, port):
            logger.info("Kafka is reachable on attempt %s", attempt)
            return True
        logger.info("Kafka not reachable (%s/%s)", attempt, max_attempts)
        time.sleep(delay_sec)

    return False


def create_producer():
    broker = os.getenv("KAFKA_BROKER", "kafka:29092")
    try:
        return KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            retries=5,
        )
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available at %s", broker)
        return None


def stream_dataset(producer, topic, dataset_path: Path, delay_sec=0.01, loop_forever=False):
    delimiter = detect_delimiter(dataset_path)
    sent = 0

    while True:
        logger.info("Streaming dataset from %s (delimiter=%s)", dataset_path, delimiter)
        with dataset_path.open("r", encoding="utf-8", errors="ignore", newline="") as csv_file:
            reader = csv.DictReader(csv_file, delimiter=delimiter)
            for row in reader:
                event = normalize_row(row)
                producer.send(topic, value=event)
                sent += 1
                if sent % 1000 == 0:
                    logger.info("Sent %s messages", sent)
                if delay_sec > 0:
                    time.sleep(delay_sec)

        producer.flush()
        logger.info("Completed one pass, total sent: %s", sent)
        if not loop_forever:
            break


def main():
    topic = os.getenv("KAFKA_TOPIC", "spotify_tracks_events")
    dataset_path = Path(os.getenv("DATASET_PATH", "/app/data/spotify_dataset_clear.csv"))
    delay_sec = float(os.getenv("EVENT_DELAY_SEC", "0.01"))
    loop_forever = os.getenv("LOOP_FOREVER", "false").lower() == "true"

    if not dataset_path.exists():
        logger.error("Dataset not found: %s", dataset_path)
        return

    if not wait_for_kafka():
        logger.error("Kafka is not available")
        return

    producer = create_producer()
    if producer is None:
        return

    stream_dataset(producer, topic, dataset_path, delay_sec=delay_sec, loop_forever=loop_forever)


if __name__ == "__main__":
    main()
