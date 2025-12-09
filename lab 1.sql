-- Lab 1
SHOW DATABASES;

DESC s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

CREATE OR REPLACE TABLE uk_prices_temp
ENGINE = Memory
AS
    SELECT *
    FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst')
    LIMIT 100;

DROP TABLE uk_prices_temp;

SHOW CREATE TABLE uk_prices_temp;

CREATE TABLE uk_prices_1
(
    `id` Nullable(String),
    `price` Nullable(String),
    `date` DateTime,
    `postcode` Nullable(String),
    `type` Nullable(String),
    `is_new` Nullable(String),
    `duration` Nullable(String),
    `addr1` Nullable(String),
    `addr2` Nullable(String),
    `street` Nullable(String),
    `locality` Nullable(String),
    `town` Nullable(String),
    `district` Nullable(String),
    `county` Nullable(String),
    `column15` Nullable(String),
    `column16` Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY date;

INSERT INTO uk_prices_1
    SELECT *
    FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices/uk_prices.csv.zst');

SELECT count()
FROM uk_prices_1;

SELECT avg(toUInt32(price))
FROM uk_prices_1;

SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE toYear(date) >= '2020';

SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE town = 'LONDON';
