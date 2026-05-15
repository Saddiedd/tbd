CREATE TABLE IF NOT EXISTS students_studies_kafka
(
    hours_studied UInt16,
    attendance UInt8,
    parental_involvement String,
    access_to_resources String,
    extracurricular_activities String,
    sleep_hours UInt8,
    previous_scores UInt8,
    motivation_level String,
    internet_access String,
    tutoring_sessions UInt8,
    family_income String,
    teacher_quality String,
    school_type String,
    peer_influence String,
    physical_activity UInt8,
    learning_disabilities String,
    parental_education_level String,
    distance_from_home String,
    gender String,
    exam_score UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'students_events',
    kafka_group_name = 'clickhouse_students_consumer_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;










CREATE TABLE IF NOT EXISTS students_home_factors_mt
(
    parental_involvement LowCardinality(String),
    internet_access LowCardinality(String),
    family_income LowCardinality(String),
    parental_education_level LowCardinality(String),
    distance_from_home LowCardinality(String),
    sleep_hours_group LowCardinality(String),
    exam_score UInt8
)
ENGINE = MergeTree
ORDER BY
(
    parental_involvement,
    internet_access,
    family_income,
    parental_education_level,
    distance_from_home,
    sleep_hours_group
);







CREATE MATERIALIZED VIEW IF NOT EXISTS students_home_factors_mv
TO students_home_factors_mt
AS
SELECT
    parental_involvement,
    internet_access,
    family_income,
    parental_education_level,
    distance_from_home,
    multiIf(
        sleep_hours < 4, '0-4 hours',
        sleep_hours < 8, '4-7 hours',
        sleep_hours < 12, '8-11 hours',
        '12+ hours'
    ) AS sleep_hours_group,
exam_score
FROM students_studies_kafka;
