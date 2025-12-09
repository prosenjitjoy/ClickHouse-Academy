-- Lab 3
DESC s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
SETTINGS schema_inference_make_columns_nullable=0;

CREATE OR REPLACE TABLE weather_temp
ENGINE = Memory
AS
    SELECT *
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
    LIMIT 100
    SETTINGS schema_inference_make_columns_nullable=0;

SHOW CREATE TABLE weather_temp;

CREATE TABLE weather
(
    `station_id` LowCardinality(String),
    `date` Date32,
    `tempAvg` Int32,
    `tempMax` Int32,
    `tempMin` Int32,
    `precipitation` Int32,
    `snowfall` Int32,
    `snowDepth` Int32,
    `percentDailySun` Int8,
    `averageWindSpeed` Int32,
    `maxWindSpeed` Int32,
    `weatherType` UInt8,
    `location` Tuple(
        `1` Float64,
        `2` Float64),
    `elevation` Float32,
    `name` LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY date;

INSERT INTO weather
    SELECT *
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
    WHERE toYear(date) >= '1995';

SELECT formatReadableQuantity(count())
FROM weather;
