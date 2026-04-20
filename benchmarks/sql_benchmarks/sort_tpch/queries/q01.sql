-- Q1: 1 sort key (type: INTEGER, cardinality: 7) + 1 payload column
SELECT l_linenumber, l_partkey
FROM lineitem
ORDER BY l_linenumber
${LIMIT:-false|LIMIT 100| }