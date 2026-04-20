-- Q3: 1 sort key (type: VARCHAR, cardinality: 4.5M) + 1 payload column
SELECT l_comment, l_partkey
FROM lineitem
ORDER BY l_comment
${LIMIT:-false|LIMIT 100| }