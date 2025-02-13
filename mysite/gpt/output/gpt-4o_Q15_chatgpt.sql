```sql
WITH combined_revenue AS (
    SELECT wl_suppkey AS s_suppkey, 
           s_name, 
           s_address, 
           s_phone, 
           SUM(wl_extendedprice * (1 - wl_discount)) AS total_revenue
    FROM web_lineitem
    JOIN supplier ON supplier.s_suppkey = web_lineitem.wl_suppkey
    WHERE wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY wl_suppkey, s_name, s_address, s_phone

    UNION ALL

    SELECT sl_suppkey AS s_suppkey, 
           s_name, 
           s_address, 
           s_phone, 
           SUM(sl_extendedprice * (1 - sl_discount)) AS total_revenue
    FROM store_lineitem
    JOIN supplier ON supplier.s_suppkey = store_lineitem.sl_suppkey
    WHERE sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY sl_suppkey, s_name, s_address, s_phone
)

SELECT s_suppkey, s_name, s_address, s_phone, SUM(total_revenue) AS total_revenue
FROM combined_revenue
GROUP BY s_suppkey, s_name, s_address, s_phone
HAVING SUM(total_revenue) = (
    SELECT MAX(total_revenue_sum)
    FROM (
        SELECT SUM(total_revenue) AS total_revenue_sum
        FROM combined_revenue
        GROUP BY s_suppkey
    ) AS revenue_sums
)
ORDER BY s_suppkey;
```

-- Prompt Token count = 1019

```sql
WITH combined_revenue AS (
    SELECT wl_suppkey AS s_suppkey, 
           s_name, 
           s_address, 
           s_phone, 
           SUM(wl_extendedprice * (1 - wl_discount)) AS total_revenue
    FROM web_lineitem
    JOIN supplier ON supplier.s_suppkey = web_lineitem.wl_suppkey
    WHERE wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY wl_suppkey, s_name, s_address, s_phone

    UNION ALL

    SELECT sl_suppkey AS s_suppkey, 
           s_name, 
           s_address, 
           s_phone, 
           SUM(sl_extendedprice * (1 - sl_discount)) AS total_revenue
    FROM store_lineitem
    JOIN supplier ON supplier.s_suppkey = store_lineitem.sl_suppkey
    WHERE sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY sl_suppkey, s_name, s_address, s_phone
)

SELECT s_suppkey, s_name, s_address, s_phone, SUM(total_revenue) AS total_revenue
FROM combined_revenue
GROUP BY s_suppkey, s_name, s_address, s_phone
HAVING SUM(total_revenue) = (
    SELECT MAX(total_revenue_sum) 
    FROM (
        SELECT SUM(total_revenue) AS total_revenue_sum
        FROM combined_revenue
        GROUP BY s_suppkey
    ) AS revenue_sums
)
ORDER BY s_suppkey;
```

-- Prompt Token count = 1440

