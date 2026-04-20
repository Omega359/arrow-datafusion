-- Q2: 1 sort key (type: BIGINT, cardinality: 1.5M) + 1 payload column
SELECT l_orderkey, l_partkey
FROM lineitem
ORDER BY l_orderkey
${LIMIT:-false|LIMIT 100| }