# Лабораторная №5: Spotify Dataset + Kafka + ClickHouse + Metabase

## 1. Описание проекта
Потоковый ETL-конвейер для бизнес-процесса №1 «Анализ популярности треков».

## 2. Архитектура
`Data Generator -> Kafka -> ClickHouse Kafka Engine -> Materialized View -> MergeTree -> Metabase`

## 3. Почему полный датасет не в GitHub
Полный CSV слишком большой, поэтому в репозитории только `data_sample/spotify_dataset_sample.csv`.

## 4. Как положить полный датасет локально
1. Положите файл в `./data/spotify_dataset_clear.csv`.
2. Если очищенного файла нет — используйте `./data/spotify_dataset.csv` и скорректируйте `DATASET_PATH` + volume в `docker-compose.yml`.

## 5. Как запустить проект
```bash
docker compose down -v
docker compose up --build -d
docker compose logs -f
```

Быстрый режим загрузки:
- `EVENT_DELAY_SEC: "0"`
- `LOOP_FOREVER: "false"`

Демо-поток:
- `EVENT_DELAY_SEC: "0.05"`
- `LOOP_FOREVER: "false"`

## 6. Как проверить Kafka
```bash
docker compose ps

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic spotify_tracks_events \
  --from-beginning \
  --max-messages 5
```

## 7. Как проверить ClickHouse
```bash
docker exec -it clickhouse clickhouse-client --query "SHOW TABLES"
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM spotify_genre_popularity_mt"
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM spotify_emotion_popularity_mt"
```

Доп. проверки в `clickhouse/queries/checks.sql`.

## 8. Как открыть Metabase
- URL: http://localhost:3000
- Database type: ClickHouse
- Host: `clickhouse`
- Port: `8123`
- Database name: `default`
- Username: `default`
- Password: пусто

## 9. SQL-запросы для диаграмм
Файлы в `clickhouse/queries/`:
- `metabase_genre_popularity.sql`
- `metabase_emotion_popularity.sql`
- `metabase_genre_audio_features.sql`
- `metabase_emotion_audio_features.sql`

Обязательные диаграммы:
1. Жанр -> средняя популярность
2. Эмоция -> средняя популярность

## 10. Как сравнивать с практикой №1 и лабораторной №2
Сравнивается **один и тот же вопрос** на 3 источниках:
- Практика №1: CSV + pandas
- Лабораторная №2: `mart_popularity_by_genre`, `mart_popularity_by_emotion`
- Лабораторная №5: `spotify_genre_popularity_mt`, `spotify_emotion_popularity_mt`

Проверьте:
1. Совпадает ли порядок жанров/эмоций по популярности.
2. Близки ли значения.
3. Есть ли расхождения и почему (частичная загрузка, другой CSV, очистка, округление, NULL).

## 11. Как запустить benchmarks
```bash
cd benchmarks
python benchmark_pandas.py
python benchmark_mysql_dwh.py
python benchmark_clickhouse.py
```
