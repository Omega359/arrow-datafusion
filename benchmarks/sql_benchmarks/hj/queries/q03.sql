-- Q3: 100% Density, 100% Hit rate
-- density: 1.0,
-- prob_hit: 1.0,
-- build_size: "100K",
-- probe_size: "60M",
SELECT s_suppkey
FROM supplier
         JOIN lineitem ON s_suppkey = l_suppkey;
