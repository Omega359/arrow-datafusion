-- Q6: 3 sort keys {(INTEGER, 7), (BIGINT, 10k), (BIGINT, 1.5M)} + 1 payload column
SELECT l_linenumber, l_suppkey, l_orderkey, l_partkey
FROM lineitem
ORDER BY l_linenumber, l_suppkey, l_orderkey
${LIMIT:-false|LIMIT 100| }