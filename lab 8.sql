-- Lab 8
SELECT
    count(),
    avg(price)
FROM uk_prices_3
WHERE toYear(date) = '2020';

WITH
    toYear(date) AS year
SELECT
    year,
    count(),
    avg(price)
FROM uk_prices_3
GROUP BY year
ORDER BY year ASC;

CREATE TABLE prices_by_year_dest (
    price UInt32,
    date Date,
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date);

CREATE MATERIALIZED VIEW prices_by_year_view
TO prices_by_year_dest
AS
    SELECT
        price,
        date,
        addr1,
        addr2,
        street,
        town,
        district,
        county
    FROM uk_prices_3;

INSERT INTO prices_by_year_dest
    SELECT
        price,
        date,
        addr1,
        addr2,
        street,
        town,
        district,
        county
    FROM uk_prices_3;

SELECT count()
FROM prices_by_year_dest;

SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

SELECT * FROM system.parts
WHERE table='uk_prices_3';

SELECT
    count(),
    avg(price)
FROM prices_by_year_dest
WHERE toYear(date) = '2020';

SELECT
    count(),
    max(price),
    avg(price),
    quantile(0.90)(price)
FROM prices_by_year_dest
WHERE county = 'STAFFORDSHIRE'
    AND date >= toDate('2005-06-01') AND date <= toDate('2005-06-30');

INSERT INTO uk_prices_3 VALUES
    ('51f279f5-ef5f-46e1-bd8e-b6c4159d8fa7', 125000, '1994-03-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    ('a0d2f609-b6f9-4972-857c-8e4266d146ae', 440000000, '1994-07-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    ('1017aff1-6f1e-420a-aad5-7d03ce60c8c5', 2000000, '1994-01-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');

SELECT * FROM prices_by_year_dest
WHERE toYear(date) = '1994';

SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

CREATE TABLE uk_averages_by_day (
    day LowCardinality(String),
    average_price UInt32
)
ENGINE = MergeTree
PRIMARY KEY day;

CREATE MATERIALIZED VIEW uk_averages_by_day_mv
REFRESH EVERY 12 HOURS
TO uk_averages_by_day
AS
    SELECT
        toYYYYMMDD(date) AS day,
        avg(price) AS average_price
    FROM uk_prices_3
    WHERE toYear(date) >= '2025'
    GROUP BY day;

SELECT *
FROM uk_averages_by_day;


SELECT
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_prices_3
GROUP BY town
ORDER BY sum_price DESC;

CREATE TABLE prices_sum_dest
(
    town LowCardinality(String),
    sum_price UInt64
)
ENGINE = SummingMergeTree
PRIMARY KEY town;

CREATE MATERIALIZED VIEW prices_sum_view
TO prices_sum_dest
AS
    SELECT
        town,
        sum(price) AS sum_price
    FROM uk_prices_3
    GROUP BY town;

INSERT INTO prices_sum_dest
    SELECT
        town,
        sum(price) AS sum_price
    FROM uk_prices_3
    GROUP BY town;

SELECT count()
FROM prices_sum_dest;

SELECT
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_prices_3
WHERE town = 'LONDON'
GROUP BY town;

SELECT
    town,
    sum_price AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON';

INSERT INTO uk_prices_3 (price, date, town, street)
VALUES
    (4294967295, toDate('1994-01-01'), 'LONDON', 'My Street1');


SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON'
GROUP BY town;

SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
GROUP BY town
ORDER BY sum DESC
LIMIT 10;

WITH
    toStartOfMonth(date) AS month
SELECT
    month,
    min(price) AS min_price,
    max(price) AS max_price
FROM uk_prices_3
GROUP BY month
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT
    month,
    avg(price)
FROM uk_prices_3
GROUP BY month
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT
    month,
    count()
FROM uk_prices_3
GROUP BY month
ORDER BY month DESC;

CREATE TABLE uk_prices_aggs_dest (
    month Date,
    min_price SimpleAggregateFunction(min, UInt32),
    max_price SimpleAggregateFunction(max, UInt32),
    volume AggregateFunction(count, UInt32),
    avg_price AggregateFunction(avg, UInt32)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY month;

CREATE MATERIALIZED VIEW uk_prices_aggs_view
TO uk_prices_aggs_dest
AS
    WITH
        toStartOfMonth(date) AS month
    SELECT
        month,
        minSimpleState(price) AS min_price,
        maxSimpleState(price) AS max_price,
        countState(price) AS volume,
        avgState(price) AS avg_price
    FROM uk_prices_3
    GROUP BY month;

INSERT INTO uk_prices_aggs_dest
    WITH
        toStartOfMonth(date) AS month
    SELECT
        month,
        minSimpleState(price) AS min_price,
        maxSimpleState(price) AS max_price,
        countState(price) AS volume,
        avgState(price) AS avg_price
    FROM uk_prices_3
    WHERE date >= toDate('1995-01-01')
    GROUP BY month;

SELECT * FROM uk_prices_aggs_dest;

SELECT
    month,
    min(min_price),
    max(max_price)
FROM uk_prices_aggs_dest
WHERE
    month >= toDate('2023-01-01')
    AND month < toDate('2024-01-01')
GROUP BY month
ORDER BY month DESC;

SELECT
    month,
    avgMerge(avg_price)
FROM uk_prices_aggs_dest
WHERE
    month >= (toStartOfMonth(now()) - (INTERVAL 2 YEAR))
    AND month < toStartOfMonth(now())
GROUP BY month
ORDER BY month DESC;

SELECT
    countMerge(volume)
FROM uk_prices_aggs_dest
WHERE toYear(month) = '2020';

INSERT INTO uk_prices_3 (date, price, town) VALUES
    ('1994-08-01', 10000, 'Little Whinging'),
    ('1994-08-01', 1, 'Little Whinging');

SELECT
    month,
    countMerge(volume),
    min(min_price),
    max(max_price)
FROM uk_prices_aggs_dest
WHERE toYYYYMM(month) = '199408'
GROUP BY month;

SELECT
    toYear(date) AS year,
    count(),
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_prices_3
WHERE town = 'LIVERPOOL'
GROUP BY year
ORDER BY year DESC;

SELECT
    formatReadableSize(sum(bytes_on_disk)),
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_prices_3' AND active = 1;

ALTER TABLE uk_prices_3
    ADD PROJECTION town_date_projection (
        SELECT
            town, date, price
        ORDER BY town,date
    );


ALTER TABLE uk_prices_3
    MATERIALIZE PROJECTION town_date_projection;

ALTER TABLE uk_prices_3
    ADD PROJECTION handy_aggs_projection (
        SELECT
            avg(price),
            max(price),
            sum(price)
        GROUP BY town
    );

ALTER TABLE uk_prices_3
    MATERIALIZE PROJECTION handy_aggs_projection;

SELECT
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_prices_3
WHERE town = 'LIVERPOOL';

EXPLAIN SELECT
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_prices_3
WHERE town = 'LIVERPOOL';

SELECT DISTINCT county
FROM uk_prices_3;

SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_prices_3
WHERE county = 'GREATER LONDON';

ALTER TABLE uk_prices_3
    ADD INDEX county_index county
    TYPE bloom_filter
    GRANULARITY 1;

ALTER TABLE uk_prices_3
    MATERIALIZE INDEX county_index;

SELECT *
FROM system.mutations;

SELECT *
FROM system.mutations
WHERE table = 'uk_prices_3';

SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

EXPLAIN indexes = 1 SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_prices_3
WHERE county = 'GREATER LONDON';
