-- Q7: 3 sort keys {(INTEGER, 7), (BIGINT, 10k), (BIGINT, 1.5M)} + 12 all other columns
SELECT l_linenumber,
       l_suppkey,
       l_orderkey,
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
ORDER BY l_linenumber, l_suppkey, l_orderkey
${LIMIT:-false|LIMIT 100| }