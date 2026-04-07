-- Q10: 4 sort keys {(BIGINT, 1.5M), (BIGINT, 10k), (INTEGER, 7), (VARCHAR, 4.5M)} + 12 all other columns
SELECT l_orderkey,
       l_suppkey,
       l_linenumber,
       l_comment,
       l_partkey,
       l_quantity,
       l_extendedprice,
       l_discount,
       l_tax,
       l_returnflag,
       l_linestatus,
       l_shipdate,
       l_commitdate,
       l_receiptdate,
       l_shipinstruct,
       l_shipmode
FROM lineitem
ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment
${LIMIT|LIMIT 100| }