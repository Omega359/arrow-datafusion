-- Q11: 10% Density, 100% Hit rate
-- density: 0.1,
-- prob_hit: 1.0,
-- build_size: "100K",
-- probe_size: "60M",
SELECT l.k
FROM (SELECT l_suppkey * 10 as k
      FROM lineitem) l
         JOIN (SELECT s_suppkey * 10 as k
               FROM supplier) s ON l.k = s.k;
