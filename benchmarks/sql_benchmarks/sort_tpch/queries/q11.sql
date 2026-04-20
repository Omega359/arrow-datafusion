-- Q11: 1 sort key (type: VARCHAR, cardinality: 4.5M) + 1 payload column
SELECT l_shipmode, l_comment, l_partkey
FROM lineitem
ORDER BY l_shipmode
${LIMIT:-false|LIMIT 100| }