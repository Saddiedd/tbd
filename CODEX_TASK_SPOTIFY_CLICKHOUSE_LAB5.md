# ТЗ для Codex: лабораторная №5 ClickHouse + Kafka + Metabase для Spotify Dataset

## 1. Общий контекст

Нужно адаптировать проект лабораторной №5 под мой Spotify Dataset.

В репозитории будет папка:

```text
example/
```

Это готовый проект по общему заданию лабораторной №5, но для другого датасета. Его нужно использовать как архитектурный пример.

Итоговая архитектура должна быть такой:

```text
Data Generator → Kafka → ClickHouse Kafka Engine → Materialized View → MergeTree → Metabase
```

Смысл лабораторной: построить потоковый ETL-конвейер, в котором Python-генератор читает CSV, отправляет события в Kafka, ClickHouse читает Kafka-топик через Kafka Engine table, Materialized View переносит/преобразует данные в MergeTree-таблицы, а Metabase используется для визуализации.

---

## 2. Важное ограничение по датасету

Полный датасет нельзя залить на GitHub из-за размера.

Поэтому в репозитории будет только маленький пример первых строк:

```text
data_sample/spotify_dataset_sample.csv
```

Полный датасет будет лежать локально у пользователя:

```text
data/spotify_dataset_clear.csv
```

или, если очищенного файла нет:

```text
data/spotify_dataset.csv
```

Codex должен использовать `data_sample/spotify_dataset_sample.csv`, чтобы определить:

```text
названия колонок
типы данных
формат значений
правила преобразования
```

Но весь код должен быть написан так, чтобы в реальном запуске использовать полный локальный файл через Docker volume.

В `docker-compose.yml` путь к полному датасету должен задаваться так:

```yaml
volumes:
  - ./data/spotify_dataset_clear.csv:/app/data/spotify_dataset_clear.csv:ro
```

И через переменную окружения:

```yaml
DATASET_PATH: "/app/data/spotify_dataset_clear.csv"
```

Если используется сырой датасет:

```yaml
volumes:
  - ./data/spotify_dataset.csv:/app/data/spotify_dataset.csv:ro
```

```yaml
DATASET_PATH: "/app/data/spotify_dataset.csv"
```

---

## 3. Файлы, которые будут в репозитории

### 3.1 Пример проекта

```text
example/
```

Это проект общего задания по лабораторной №5. Нужно взять из него структуру и адаптировать под Spotify Dataset.

### 3.2 Sample датасета

```text
data_sample/spotify_dataset_sample.csv
```

Это первые 20–50 строк датасета. Использовать его только для понимания структуры колонок.

### 3.3 Полный датасет

Полный датасет не хранится в GitHub. Он будет лежать локально:

```text
data/spotify_dataset_clear.csv
```

или:

```text
data/spotify_dataset.csv
```

Папка `data/` должна быть добавлена в `.gitignore`.

### 3.4 Файлы лабораторной №2

```text
script.py
create_dwh.py
create_marts.py
1.docx
```

Эти файлы нужно использовать как источник логики бизнес-процесса №1.

Файл `script.py` парсит Spotify Dataset в звездообразную структуру: создаёт справочники артистов, песен, эмоций, жанров, альбомов, дат релиза, тональностей и размеров, а также факт-таблицу `fact_tracks_denorm`.

В факт-таблице используются поля:

```text
popularity
energy
danceability
positiveness
speechiness
liveness
acousticness
instrumentalness
tempo
loudness_db
```

Файл `create_dwh.py` создаёт DWH-базу `music_bp1_dwh`, измерения:

```text
dim_artists
dim_emotions
dim_genres
dim_albums
```

и факт-таблицу:

```text
fact_track_popularity
```

Файл `create_marts.py` создаёт две data mart:

```text
mart_popularity_by_genre
mart_popularity_by_emotion
```

В них считаются:

```text
track_count
avg_popularity
avg_energy
avg_danceability
avg_positiveness
avg_speechiness
avg_liveness
avg_acousticness
avg_instrumentalness
avg_tempo
avg_loudness_db
```

Эти витрины нужно использовать как основу для сравнения с ClickHouse.

---

## 4. Бизнес-процесс

Работаем с бизнес-процессом №1:

```text
Анализ популярности треков
```

Основные вопросы:

```text
1. Как жанр влияет на популярность?
2. Как эмоция влияет на популярность?
```

Для каждого вопроса нужно получить аналогичные результаты в трёх вариантах:

```text
Практика №1: pandas по сырому CSV
Лабораторная №2: SQL-запросы к data_mart
Лабораторная №5: ClickHouse MergeTree-таблицы
```

---

## 5. Что нужно сделать в проекте

Нужно создать рабочий проект лабораторной №5 для Spotify Dataset.

Ожидаемая структура:

```text
.
├── docker-compose.yml
├── .gitignore
├── README.md
├── data_sample/
│   └── spotify_dataset_sample.csv
├── data/
│   └── spotify_dataset_clear.csv        # локально, НЕ в git
├── data-generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── clickhouse/
│   ├── init.sql
│   └── queries/
│       ├── metabase_genre_popularity.sql
│       ├── metabase_emotion_popularity.sql
│       ├── metabase_genre_audio_features.sql
│       ├── metabase_emotion_audio_features.sql
│       ├── comparison_queries.sql
│       └── checks.sql
├── metabase/
│   ├── Dockerfile
│   └── clickhouse.metabase-driver.jar
└── benchmarks/
    ├── benchmark_pandas.py
    ├── benchmark_mysql_dwh.py
    ├── benchmark_clickhouse.py
    └── README.md
```

---

## 6. `.gitignore`

Создать или обновить `.gitignore`.

Обязательно исключить полный датасет:

```gitignore
data/
*.csv
!data_sample/*.csv
```

То есть все большие CSV не должны попадать в GitHub, но sample из `data_sample/` должен оставаться в репозитории.

---

## 7. Docker Compose

Создать `docker-compose.yml` по аналогии с `example`.

Должны быть сервисы:

```text
zookeeper
kafka
clickhouse
data-generator
metabase
```

Для `zookeeper` и `kafka` использовать образы:

```text
confluentinc/cp-zookeeper:7.3.0
confluentinc/cp-kafka:7.3.0
```

Если используются эти версии, добавить в оба сервиса:

```yaml
JAVA_TOOL_OPTIONS: "-Djdk.disableContainerMetrics=true"
```

Это нужно для обхода возможной ошибки Java/cgroup на новых Linux-системах.

Пример настроек Kafka:

```yaml
KAFKA_BROKER_ID: 1
KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092"
KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092"
KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Для `data-generator`:

```yaml
environment:
  KAFKA_BROKER: "kafka:29092"
  KAFKA_TOPIC: "spotify_tracks_events"
  DATASET_PATH: "/app/data/spotify_dataset_clear.csv"
  EVENT_DELAY_SEC: "0.01"
  LOOP_FOREVER: "false"
volumes:
  - ./data/spotify_dataset_clear.csv:/app/data/spotify_dataset_clear.csv:ro
```

Нужно предусмотреть режим быстрой загрузки:

```yaml
EVENT_DELAY_SEC: "0"
LOOP_FOREVER: "false"
```

И режим демонстрации потоковой загрузки:

```yaml
EVENT_DELAY_SEC: "0.05"
LOOP_FOREVER: "false"
```

---

## 8. Data Generator

Создать:

```text
data-generator/app.py
```

Генератор должен:

```text
1. Читать CSV по пути из DATASET_PATH.
2. Определять разделитель CSV автоматически, если возможно.
3. Нормализовать строки.
4. Преобразовывать значения в корректные типы.
5. Ждать доступности Kafka.
6. Создавать KafkaProducer.
7. Отправлять каждую строку как JSON в Kafka topic.
8. Логировать прогресс загрузки.
9. Поддерживать EVENT_DELAY_SEC.
10. Поддерживать LOOP_FOREVER.
```

`requirements.txt`:

```txt
kafka-python==2.0.2
```

Если используются дополнительные библиотеки, добавить их явно.

---

## 9. Правила преобразования полей

Использовать логику из `script.py`.

Основные соответствия колонок:

```text
Artist(s) → artist
song → song
emotion → emotion
Genre → genre
Album → album
Release Date → release_date
Length → track_length_seconds
Tempo → tempo
Loudness (db) → loudness_db
Explicit → explicit_flag
Popularity → popularity
Energy → energy
Danceability → danceability
Positiveness → positiveness
Speechiness → speechiness
Liveness → liveness
Acousticness → acousticness
Instrumentalness → instrumentalness
Good for Party → good_for_party
Good for Work/Study → good_for_work_study
Good for Relaxation/Meditation → good_for_relaxation_meditation
Good for Exercise → good_for_exercise
Good for Running → good_for_running
Good for Yoga/Stretching → good_for_yoga_stretching
Good for Driving → good_for_driving
Good for Social Gatherings → good_for_social_gatherings
Good for Morning Routine → good_for_morning_routine
```

Нужные преобразования:

```text
Length → секунды
Explicit → 0/1
Loudness (db) → число без "db"
Popularity → int
Tempo → float или int
Energy/Danceability/etc. → float
пустые значения → null
```

Код должен быть устойчивым к отсутствующим значениям.

---

## 10. ClickHouse init.sql

Создать:

```text
clickhouse/init.sql
```

В нём должны быть:

```text
1. Kafka Engine table
2. MergeTree table для анализа популярности по жанру
3. Materialized View для переноса данных по жанрам
4. MergeTree table для анализа популярности по эмоции
5. Materialized View для переноса данных по эмоциям
```

### 10.1 Kafka Engine table

Создать таблицу:

```sql
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
```

### 10.2 MergeTree для жанров

Создать таблицу:

```sql
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
```

Важно: поле `loaded_at` обязательно. По индивидуальному заданию нужно добавить время загрузки на этапе MV. Можно добавить `loaded_at DateTime DEFAULT now()` или `now() AS loaded_at` в Materialized View.

### 10.3 Materialized View для жанров

```sql
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
```

### 10.4 MergeTree для эмоций

```sql
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
```

### 10.5 Materialized View для эмоций

```sql
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
```

---

## 11. SQL-запросы для Metabase

Создать папку:

```text
clickhouse/queries/
```

В ней создать SQL-файлы для построения диаграмм в Metabase.

Все запросы должны быть пригодны для копирования в Metabase.

### 11.1 Диаграмма: жанр → средняя популярность

Файл:

```text
clickhouse/queries/metabase_genre_popularity.sql
```

```sql
SELECT
    genre,
    count() AS track_count,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_genre_popularity_mt
WHERE genre IS NOT NULL AND genre != ''
GROUP BY genre
HAVING track_count >= 5
ORDER BY avg_popularity DESC
LIMIT 20;
```

Рекомендуемая диаграмма:

```text
Bar chart
X-axis: genre
Y-axis: avg_popularity
```

### 11.2 Диаграмма: эмоция → средняя популярность

Файл:

```text
clickhouse/queries/metabase_emotion_popularity.sql
```

```sql
SELECT
    emotion,
    count() AS track_count,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_emotion_popularity_mt
WHERE emotion IS NOT NULL AND emotion != ''
GROUP BY emotion
ORDER BY avg_popularity DESC;
```

Рекомендуемая диаграмма:

```text
Bar chart
X-axis: emotion
Y-axis: avg_popularity
```

### 11.3 Диаграмма: аудио-характеристики по жанрам

Файл:

```text
clickhouse/queries/metabase_genre_audio_features.sql
```

```sql
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
```

Рекомендуемая диаграмма:

```text
Grouped bar chart
```

### 11.4 Диаграмма: аудио-характеристики по эмоциям

Файл:

```text
clickhouse/queries/metabase_emotion_audio_features.sql
```

```sql
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
```

---

## 12. SQL-запросы для сравнения с data_mart лабораторной №2

Создать файл:

```text
clickhouse/queries/comparison_queries.sql
```

В нём сделать ClickHouse-запросы, аналогичные витринам из `create_marts.py`.

### 12.1 Аналог `mart_popularity_by_genre`

```sql
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
```

### 12.2 Аналог `mart_popularity_by_emotion`

```sql
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
```

---

## 13. Скрипты проверки ClickHouse

Создать файл:

```text
clickhouse/queries/checks.sql
```

Содержимое:

```sql
SHOW TABLES;

SELECT count() AS rows_in_genre_mt
FROM spotify_genre_popularity_mt;

SELECT count() AS rows_in_emotion_mt
FROM spotify_emotion_popularity_mt;

SELECT *
FROM spotify_genre_popularity_mt
LIMIT 10;

SELECT *
FROM spotify_emotion_popularity_mt
LIMIT 10;

SELECT
    min(loaded_at) AS first_loaded_at,
    max(loaded_at) AS last_loaded_at,
    count() AS rows_count
FROM spotify_genre_popularity_mt;
```

---

## 14. Benchmark: сравнение скорости выполнения запросов

Создать папку:

```text
benchmarks/
```

Создать 3 скрипта:

```text
benchmark_pandas.py
benchmark_mysql_dwh.py
benchmark_clickhouse.py
```

Нужно сравнить время выполнения одинаковых аналитических запросов:

```text
1. Средняя популярность по жанрам
2. Средняя популярность по эмоциям
```

### 14.1 benchmark_pandas.py

Скрипт должен:

```text
1. Читать полный локальный CSV из ../data/spotify_dataset_clear.csv.
2. Если файла нет, пробовать ../data/spotify_dataset.csv.
3. Замерять время группировки по жанру.
4. Замерять время группировки по эмоции.
5. Выводить время выполнения.
```

Пример логики:

```python
import time
import pandas as pd
from pathlib import Path

DATASET_CLEAR = Path("../data/spotify_dataset_clear.csv")
DATASET_RAW = Path("../data/spotify_dataset.csv")

dataset_path = DATASET_CLEAR if DATASET_CLEAR.exists() else DATASET_RAW

df = pd.read_csv(dataset_path)

start = time.perf_counter()
genre_result = (
    df.groupby("Genre")["Popularity"]
    .mean()
    .reset_index()
    .sort_values("Popularity", ascending=False)
)
genre_time = time.perf_counter() - start

start = time.perf_counter()
emotion_result = (
    df.groupby("emotion")["Popularity"]
    .mean()
    .reset_index()
    .sort_values("Popularity", ascending=False)
)
emotion_time = time.perf_counter() - start

print("Pandas genre query time:", genre_time)
print("Pandas emotion query time:", emotion_time)
```

Добавить защиту на случай, если названия колонок отличаются.

### 14.2 benchmark_mysql_dwh.py

Скрипт должен подключаться к MySQL/MariaDB базе:

```text
music_bp1_dwh
```

И замерять запросы к витринам:

```sql
SELECT genre_name, avg_popularity
FROM mart_popularity_by_genre
ORDER BY avg_popularity DESC;
```

```sql
SELECT emotion_name, avg_popularity
FROM mart_popularity_by_emotion
ORDER BY avg_popularity DESC;
```

Использовать `time.perf_counter()`.

### 14.3 benchmark_clickhouse.py

Скрипт должен подключаться к ClickHouse через HTTP:

```text
http://localhost:8123
```

Можно использовать `requests`.

Запросы:

```sql
SELECT
    genre,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_genre_popularity_mt
GROUP BY genre
ORDER BY avg_popularity DESC
FORMAT JSON;
```

```sql
SELECT
    emotion,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_emotion_popularity_mt
GROUP BY emotion
ORDER BY avg_popularity DESC
FORMAT JSON;
```

Также добавить в README вариант чистого замера через `FORMAT Null`:

```sql
SELECT
    genre,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_genre_popularity_mt
GROUP BY genre
ORDER BY avg_popularity DESC
FORMAT Null;
```

---

## 15. Metabase

Metabase должен запускаться на:

```text
http://localhost:3000
```

ClickHouse подключать так:

```text
Database type: ClickHouse
Host: clickhouse
Port: 8123
Database name: default
Username: default
Password: пусто
```

SQL-запросы для диаграмм брать из:

```text
clickhouse/queries/
```

Нужно подготовить диаграммы:

```text
1. Жанр → средняя популярность
2. Эмоция → средняя популярность
3. Жанр → средние аудио-характеристики
4. Эмоция → средние аудио-характеристики
```

Главные обязательные диаграммы:

```text
Жанр → средняя популярность
Эмоция → средняя популярность
```

Они соответствуют бизнес-процессу №1.

---

## 16. README.md

Создать подробный README.md.

В README должны быть разделы:

```text
1. Описание проекта
2. Архитектура
3. Почему полный датасет не в GitHub
4. Как положить полный датасет локально
5. Как запустить проект
6. Как проверить Kafka
7. Как проверить ClickHouse
8. Как открыть Metabase
9. Какие SQL-запросы использовать для диаграмм
10. Как сравнивать с практикой №1 и лабораторной №2
11. Как запустить benchmarks
```

Команды запуска:

```bash
docker compose down -v
docker compose up --build -d
docker compose logs -f
```

Проверка контейнеров:

```bash
docker compose ps
```

Проверка Kafka topic:

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Проверка сообщений Kafka:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic spotify_tracks_events \
  --from-beginning \
  --max-messages 5
```

Проверка ClickHouse:

```bash
docker exec -it clickhouse clickhouse-client --query "SHOW TABLES"
```

Проверка строк:

```bash
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM spotify_genre_popularity_mt"
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM spotify_emotion_popularity_mt"
```

---

## 17. Как сравнивать диаграммы

В README объяснить, что сравниваются не разные графики, а один и тот же аналитический вопрос на трёх источниках:

```text
Практика №1:
CSV + pandas

Лабораторная №2:
MySQL/MariaDB data_mart

Лабораторная №5:
ClickHouse MergeTree
```

Сравнение 1:

```text
Средняя популярность по жанру
```

Источники:

```text
pandas: spotify_dataset_clear.csv
data_mart: mart_popularity_by_genre
ClickHouse: spotify_genre_popularity_mt
```

Сравнение 2:

```text
Средняя популярность по эмоции
```

Источники:

```text
pandas: spotify_dataset_clear.csv
data_mart: mart_popularity_by_emotion
ClickHouse: spotify_emotion_popularity_mt
```

В отчёте пользователь должен проверить:

```text
1. Совпадает ли порядок жанров/эмоций по средней популярности.
2. Близки ли численные значения.
3. Есть ли расхождения.
4. Если есть расхождения, объяснить их:
   - не все строки загружены в ClickHouse;
   - использовался другой файл CSV;
   - разные правила очистки;
   - округление;
   - NULL-значения.
```

---

## 18. Требования к качеству

1. Не изменять и не удалять исходные файлы лабораторной №2:
   - `script.py`
   - `create_dwh.py`
   - `create_marts.py`

2. Использовать их как источник логики.

3. Все новые файлы создавать отдельно.

4. Все SQL-запросы должны быть пригодны для копирования в Metabase.

5. В ClickHouse обязательно использовать `loaded_at`.

6. Должен быть быстрый режим загрузки.

7. Должны быть проверки количества строк.

8. Должны быть benchmark-скрипты.

9. Полный датасет не должен попадать в GitHub.

10. `data_sample/spotify_dataset_sample.csv` должен использоваться только для анализа структуры.

---

## 19. Приоритет выполнения

Сначала сделать минимально рабочий поток:

```text
CSV → data-generator → Kafka → ClickHouse Kafka table → MV → MergeTree
```

Потом добавить:

```text
Metabase
SQL-запросы для диаграмм
benchmark-скрипты
README
```

Не усложнять ClickHouse до звездообразной схемы. Для лабораторной №5 использовать упрощённый подход, как в методичке: широкие MergeTree-таблицы вместо отдельных таблиц измерений.

---

## 20. Финальный результат

На выходе должны появиться:

```text
1. Рабочий docker-compose.yml
2. data-generator для Spotify Dataset
3. clickhouse/init.sql
4. SQL-запросы для Metabase
5. SQL-запросы для сравнения с data_mart лабораторной №2
6. Скрипты benchmark для pandas, MySQL DWH и ClickHouse
7. README.md с инструкцией запуска
8. .gitignore, исключающий полный датасет
```

Главные обязательные аналитические диаграммы:

```text
1. Жанр → средняя популярность
2. Эмоция → средняя популярность
```

Они должны быть сопоставимы с:

```text
1. Практикой №1 на pandas
2. Data marts из лабораторной №2
3. ClickHouse MergeTree-таблицами из лабораторной №5
```
