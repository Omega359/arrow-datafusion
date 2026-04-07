-- Q8: 4 sort keys {(BIGINT, 1.5M), (BIGINT, 10k), (INTEGER, 7), (VARCHAR, 4.5M)} + no payload column
SELECT l_orderkey, l_suppkey, l_linenumber, l_comment
FROM lineitem
ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment
${LIMIT|LIMIT 100| }