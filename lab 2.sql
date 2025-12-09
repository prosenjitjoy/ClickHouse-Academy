-- Lab 2
SELECT *
FROM system.parts
WHERE table = 'uk_prices_1'
AND active = 1;

SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'uk_prices_1' AND active = 1;

SELECT avg(toUInt32(price))
FROM uk_prices_1
WHERE toYYYYMM(date) = '202207';

CREATE TABLE uk_prices_2
(
    `id` Nullable(String),
    `price` Nullable(String),
    `date` DateTime,
    `postcode` String,
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
PRIMARY KEY (postcode, date);

INSERT INTO uk_prices_2
    SELECT * FROM uk_prices_1;

SELECT max(toUInt32(price))
FROM uk_prices_2
WHERE postcode = 'LU1 5FT';

SELECT avg(toUInt32(price))
FROM uk_prices_2
WHERE toYear(date) >= '2020';

SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE table = 'uk_prices_2' AND active = 1;
