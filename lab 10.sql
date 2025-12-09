-- Lab 10
SELECT
    formatReadableSize(sum(data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio,
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_prices_3' AND active = 1;

SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'uk_prices_3' AND active = 1
GROUP BY column;

CREATE TABLE prices_1
(
    `id`    UUID,
    `price` UInt32,
    `date` Date,
    `postcode1` LowCardinality(String) ,
    `postcode2` LowCardinality(String),
    `type` Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    `is_new` UInt8,
    `duration` Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    `addr1` String,
    `addr2` String,
    `street` LowCardinality(String),
    `locality` LowCardinality(String),
    `town` LowCardinality(String),
    `district` LowCardinality(String),
    `county` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, date)
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

INSERT INTO prices_1
    SELECT * FROM uk_prices_3;

SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_1' AND active = 1
GROUP BY column;

CREATE OR REPLACE TABLE prices_2
(
    `price` UInt32 CODEC(T64, LZ4),
    `date` Date CODEC(DoubleDelta, ZSTD),
    `postcode1` String,
    `postcode2` String,
    `is_new` UInt8 CODEC(LZ4HC)
)
ENGINE = MergeTree
ORDER BY date
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

INSERT INTO prices_2
    SELECT price, date, postcode1, postcode2, is_new FROM uk_prices_3;

SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_2' AND active = 1
GROUP BY column;

CREATE OR REPLACE TABLE ttl_demo (
    key UInt32,
    value String,
    timestamp DateTime
)
ENGINE = MergeTree
ORDER BY key
TTL timestamp + INTERVAL 60 SECOND;

INSERT INTO ttl_demo VALUES
    (1, 'row1', now()),
    (2, 'row2', now());

SELECT * FROM ttl_demo;

ALTER TABLE ttl_demo
MATERIALIZE TTL;

ALTER TABLE ttl_demo
    MODIFY COLUMN value String TTL timestamp +  INTERVAL 15 SECOND;

INSERT INTO ttl_demo VALUES
    (1, 'row1', now()),
    (2, 'row2', now());

ALTER TABLE ttl_demo
MATERIALIZE TTL;

SELECT * FROM ttl_demo;

ALTER TABLE prices_1
    MODIFY TTL date + INTERVAL 5 YEAR;

ALTER TABLE prices_1
MATERIALIZE TTL;

SELECT min(date) FROM prices_1;
