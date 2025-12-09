-- Lab 5
SELECT *
FROM uk_prices_3
WHERE price >= 100_000_000
ORDER BY price desc;

SELECT count()
FROM uk_prices_3
WHERE
    price > 1_000_000
    AND date >= toDate('2024-01-01') AND date <= toDate('2024-12-31');

SELECT uniqExact(town)
FROM uk_prices_3;

SELECT
    town,
    count() AS c
FROM uk_prices_3
GROUP BY town
ORDER BY c DESC
LIMIT 1;

SELECT topKIf(10)(town, town != 'LONDON')
FROM uk_prices_3;

SELECT
    town,
    avg(price) AS avg_price
FROM uk_prices_3
GROUP BY town
ORDER BY avg_price DESC
LIMIT 10;

SELECT
    addr1,
    addr2,
    street,
    town
FROM uk_prices_3
ORDER BY price DESC
LIMIT 1;

SELECT
    avgIf(price, type = 'detached'),
    avgIf(price, type = 'semi-detached'),
    avgIf(price, type = 'terraced'),
    avgIf(price, type = 'flat'),
    avgIf(price, type = 'other')
FROM uk_prices_3;

SELECT type, avg(price) as avg_price
FROM uk_prices_3
GROUP BY type;

SELECT
    sum(price)
FROM uk_prices_3
WHERE
    county IN ['AVON','ESSEX','DEVON','KENT','CORNWALL']
    AND
    date >= toDate('2024-01-01') AND date <= toDate('2024-12-31');


SELECT
    toStartOfMonth(date) AS month,
    avg(price) AS avg_price
FROM uk_prices_3
WHERE
    date >= toDate('2005-01-01') AND date <= toDate('2010-12-31')
GROUP BY month
ORDER BY month ASC;

SELECT
    toStartOfDay(date) AS day,
    count()
FROM uk_prices_3
WHERE
    town = 'LIVERPOOL'
    AND date >= toDate('2020-01-01') AND date <= toDate('2020-12-31')
GROUP BY day
ORDER BY day ASC;

WITH (
    SELECT max(price)
    FROM uk_prices_3
) AS overall_max
SELECT
    town,
    max(price) / overall_max
FROM uk_prices_3
GROUP BY town
ORDER BY 2 DESC;
