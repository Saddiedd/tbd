import socket  # Новый импорт: нужен для проверки доступности Kafka по TCP
import csv
import json
import time
import logging
import os
from kafka import KafkaProducer  # Новый импорт: Kafka producer для отправки сообщений
from kafka.errors import NoBrokersAvailable  # Новый импорт: обработка ошибки отсутствия брокеров

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def normalize_row(row):
    return {
        "hours_studied": int(row["Hours_Studied"]),
        "attendance": int(row["Attendance"]),
        "parental_involvement": row["Parental_Involvement"],
        "access_to_resources": row["Access_to_Resources"],
        "extracurricular_activities": row["Extracurricular_Activities"],
        "sleep_hours": int(row["Sleep_Hours"]),
        "previous_scores": int(row["Previous_Scores"]),
        "motivation_level": row["Motivation_Level"],
        "internet_access": row["Internet_Access"],
        "tutoring_sessions": int(row["Tutoring_Sessions"]),
        "family_income": row["Family_Income"],
        "teacher_quality": row["Teacher_Quality"],
        "school_type": row["School_Type"],
        "peer_influence": row["Peer_Influence"],
        "physical_activity": int(row["Physical_Activity"]),
        "learning_disabilities": row["Learning_Disabilities"],
        "parental_education_level": row["Parental_Education_Level"],
        "distance_from_home": row["Distance_from_Home"],
        "gender": row["Gender"],
        "exam_score": int(row["Exam_Score"]),
    }


def check_kafka_connection(host="kafka", port=29092, timeout=5):
    """
    Новая функция.
    Проверяет TCP-доступность Kafka broker.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Socket error: {e}")
        return False


def wait_for_kafka(max_attempts=60, delay=2):
    """
    Новая функция.
    Ожидает, пока Kafka станет доступной.
    """
    broker = os.getenv("KAFKA_BROKER", "kafka:29092")  # Новая переменная окружения
    host, port = broker.split(":")
    port = int(port)

    logger.info(f"Waiting for Kafka at {broker}...")
    for attempt in range(max_attempts):
        if check_kafka_connection(host, port):
            logger.info(f"Kafka is available (attempt {attempt + 1})")
            return True
        logger.info(f"Kafka not available, attempt {attempt + 1}/{max_attempts}")
        time.sleep(delay)

    logger.error("Kafka is not available after all attempts")
    return False


def create_producer():
    """
    Новая функция.
    Создаёт объект KafkaProducer для отправки сообщений в Kafka.
    """
    bootstrap_servers = os.getenv("KAFKA_BROKER", "kafka:29092")  # Чтение адреса Kafka из переменной окружения
    logger.info(f"Connecting to Kafka: {bootstrap_servers}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            # Сериализация Python-словаря в JSON-строку и затем в bytes
            retries=5,
            max_block_ms=30000,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000,
        )
        logger.info("KafkaProducer created successfully")
        return producer
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available")
        return None
    except Exception as e:
        logger.error(f"Producer creation error: {e}")
        return None


def stream_dataset(producer, topic, file_path, delay_sec=1.0, loop_forever=False):
    """
    Функция изменена. Теперь события не просто выводятся в лог, а отправляются в Kafka.
    """
    sent = 0
    while True:
        logger.info(f"Streaming dataset from {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    event = normalize_row(row)
                    future = producer.send(topic, value=event)  # Новая строка: отправка события в Kafka
                    metadata = future.get(timeout=10)  # Ожидание подтверждения от Kafka broker
                    sent += 1

                    if sent % 10 == 0:
                        logger.info(
                            f"Sent {sent} events | "
                            f"partition={metadata.partition}, offset={metadata.offset}"
                        )  # Изменённый лог: теперь показывается факт отправки в Kafka,
                        # а также номер partition и offset

                    time.sleep(delay_sec)

                except Exception as e:
                    logger.error(f"Error sending row: {e}")  # Новая обработка ошибок при отправке
                    time.sleep(2)

        if not loop_forever:
            logger.info("Dataset completed, generator stopped")
            break
        logger.info("Dataset finished, restarting from beginning...")


def main():
    logger.info("Starting dataset-based streaming generator")

    if not wait_for_kafka():  # Новый шаг: ожидание готовности Kafka перед началом работы
        return

    producer = create_producer()  # Новый шаг: создание Kafka producer
    if not producer:
        return

    topic = os.getenv("KAFKA_TOPIC", "students_events")  # Новая переменная окружения: имя Kafka-топика
    dataset_path = os.getenv("DATASET_PATH", "/app/data/StudentPerformanceFactors.csv")
    delay_sec = float(os.getenv("EVENT_DELAY_SEC", "1.0"))
    loop_forever = os.getenv("LOOP_FOREVER", "true").lower() == "true"

    stream_dataset(
        producer=producer,  # Новый аргумент: объект Kafka producer
        topic=topic,  # Новый аргумент: имя Kafka topic
        file_path=dataset_path,
        delay_sec=delay_sec,
        loop_forever=loop_forever
    )


if __name__ == "__main__":
    main()
