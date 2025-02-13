```sql
SELECT 0.14 * SUM(w1.wl_extendedprice) AS avg_yearly
FROM part
JOIN web_lineitem w1 ON part.p_partkey = w1.wl_partkey
JOIN (
    SELECT wl_partkey, AVG(wl_quantity) * 0.7 AS threshold_quantity
    FROM web_lineitem
    GROUP BY wl_partkey
) w2 ON w1.wl_partkey = w2.wl_partkey
WHERE part.p_brand = 'Brand#53'
  AND part.p_container = 'MED BAG'
  AND w1.wl_quantity < w2.threshold_quantity;
```

-- Prompt Token count = 1883

