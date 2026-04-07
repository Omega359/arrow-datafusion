-- Q7: 50% Density, 100% Hit rate
-- density: 0.5,
-- prob_hit: 1.0,
-- build_size: "100K",
-- probe_size: "60M",
SELECT l.k
FROM (SELECT l_suppkey * 2 as k
      FROM lineitem) l
         JOIN (SELECT s_suppkey * 2 as k
               FROM supplier) s ON l.k = s.k;