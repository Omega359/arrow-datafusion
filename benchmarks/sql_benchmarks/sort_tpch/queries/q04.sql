--  Q4: 2 sort keys {(BIGINT, 1.5M), (INTEGER, 7)} + 1 payload column
SELECT l_orderkey, l_linenumber, l_partkey
FROM lineitem
ORDER BY l_orderkey, l_linenumber
${LIMIT:-false|LIMIT 100| }