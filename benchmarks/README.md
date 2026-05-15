# Benchmarks

## Запуск

```bash
python benchmark_pandas.py
python benchmark_mysql_dwh.py
python benchmark_clickhouse.py
```

## ClickHouse чистый замер (без вывода результата)

```sql
SELECT
    genre,
    round(avg(popularity), 2) AS avg_popularity
FROM spotify_genre_popularity_mt
GROUP BY genre
ORDER BY avg_popularity DESC
FORMAT Null;
```
