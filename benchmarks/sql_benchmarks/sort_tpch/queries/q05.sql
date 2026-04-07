--  Q5: 3 sort keys {(INTEGER, 7), (BIGINT, 10k), (BIGINT, 1.5M)} + no payload column
SELECT l_linenumber, l_suppkey, l_orderkey
FROM lineitem
ORDER BY l_linenumber, l_suppkey, l_orderkey
${LIMIT|LIMIT 100| }