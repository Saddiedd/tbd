FROM metabase/metabase:v0.48.6

USER root

RUN mkdir -p /plugins \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fL "https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.2.2/clickhouse.metabase-driver.jar" \
      -o /plugins/clickhouse.metabase-driver.jar \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*
