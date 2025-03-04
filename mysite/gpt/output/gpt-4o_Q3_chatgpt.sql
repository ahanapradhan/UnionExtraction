```sql
SELECT o.o_shippriority, 
       SUM(wl.wl_extendedprice * (1 - wl.wl_discount)) AS potential_revenue
FROM orders o
JOIN web_lineitem wl ON o.o_orderkey = wl.wl_orderkey
WHERE wl.wl_shipdate > :given_date
GROUP BY o.o_orderkey, o.o_shippriority
ORDER BY potential_revenue DESC
LIMIT 10;
```

-- Prompt Token count = 1584

```sql
SELECT o_shippriority, SUM(extended_revenue) AS potential_revenue
FROM (
    SELECT o.o_shippriority, (wl.wl_extendedprice * (1 - wl.wl_discount)) AS extended_revenue
    FROM orders o
    JOIN web_lineitem wl ON o.o_orderkey = wl.wl_orderkey
    WHERE wl.wl_shipdate > :given_date
    UNION ALL
    SELECT o.o_shippriority, (sl.sl_extendedprice * (1 - sl.sl_discount)) AS extended_revenue
    FROM orders o
    JOIN store_lineitem sl ON o.o_orderkey = sl.sl_orderkey
    WHERE sl.sl_shipdate > :given_date
) AS combined_revenue
GROUP BY o_shippriority
ORDER BY potential_revenue DESC
LIMIT 10;
```

-- Prompt Token count = 1593

