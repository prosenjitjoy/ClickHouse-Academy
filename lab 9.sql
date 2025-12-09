-- Lab 9
SELECT
    cluster,
    shard_num,
    replica_num,
    database_shard_name,
    database_replica_name
FROM system.clusters;

SELECT event_time, query
FROM system.query_log
ORDER BY event_time DESC
LIMIT 20;

SELECT
    event_time,
    query
FROM clusterAllReplicas(default, system.query_log)
ORDER BY  event_time DESC
LIMIT 20;

SELECT
    query,
    tables
FROM clusterAllReplicas(default, system.query_log)
WHERE has(tables, 'default.uk_prices_3');

SELECT count()
FROM clusterAllReplicas(default, system.query_log)
WHERE positionCaseInsensitive(query, 'insert') > 0;

SELECT count()
FROM system.parts;

SELECT count()
FROM clusterAllReplicas(default, system.parts);

SELECT
    instance,
    * EXCEPT instance APPLY formatReadableSize
FROM (
    SELECT
        hostname() AS instance,
        sum(primary_key_size),
        sum(primary_key_bytes_in_memory),
        sum(primary_key_bytes_in_memory_allocated)
    FROM clusterAllReplicas(default, system.parts)
    GROUP BY instance
);

SELECT
    PROJECT,
    count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_*.snappy.parquet')
GROUP BY PROJECT
ORDER BY 2 DESC
LIMIT 20;

SELECT
    PROJECT,
    count()
FROM s3Cluster(default,'https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_0_*.snappy.parquet')
GROUP BY PROJECT
ORDER BY 2 DESC
LIMIT 20;
