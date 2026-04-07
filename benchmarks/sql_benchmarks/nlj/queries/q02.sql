-- Q2: INNER 10K x 10K | Medium 20%
SELECT *
FROM range(10000) AS t1
         JOIN range(10000) AS t2
              ON (t1.value + t2.value) % 5 = 0;