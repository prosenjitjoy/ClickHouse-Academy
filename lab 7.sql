-- Lab 7
CREATE TABLE rates_monthly (
    month Date,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY month;

INSERT INTO rates_monthly
    SELECT
        toDate(date) AS month,
        variable,
        fixed,
        bank
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');

SELECT *
FROM rates_monthly;

SELECT *
FROM rates_monthly
WHERE month = '2022-05-31';

INSERT INTO rates_monthly VALUES
    ('2022-05-31', 3.2, 3.0, 1.1);

SELECT *
FROM rates_monthly
WHERE month = '2022-05-31';

SELECT *
FROM rates_monthly FINAL
WHERE month = '2022-05-31';

CREATE TABLE rates_monthly2 (
    month Date,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2),
    version UInt32
)
ENGINE = ReplacingMergeTree(version)
PRIMARY KEY month;

INSERT INTO rates_monthly2
    SELECT
        month, variable, fixed, bank, 1
    FROM rates_monthly;

INSERT INTO rates_monthly2 VALUES
    ('2022-04-30', 3.1, 2.6, 1.1, 10);

INSERT INTO rates_monthly2 VALUES
    ('2022-04-30', 2.9, 2.4, 0.9, 5);

SELECT *
FROM rates_monthly2 FINAL
WHERE month = '2022-04-30';

OPTIMIZE TABLE rates_monthly2 FINAL;

SELECT *
FROM rates_monthly2
WHERE month = '2022-04-30';


CREATE TABLE messages (
    id UInt32,
    day Date,
    message String,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
PRIMARY KEY id;

INSERT INTO messages VALUES
   (1, '2024-07-04', 'Hello', 1),
   (2, '2024-07-04', 'Hi', 1),
   (3, '2024-07-04', 'Bonjour', 1);

SELECT * FROM messages;

INSERT INTO messages VALUES
   (2, '2024-07-04', 'Hi', -1),
   (2, '2024-07-05', 'Goodbye', 1);

INSERT INTO messages (id,sign) VALUES
    (3,-1);

SELECT * FROM messages;

SELECT * FROM messages FINAL;

INSERT INTO messages VALUES
   (1, '2024-07-03', 'Adios', 1);

SELECT * FROM messages FINAL;
