```sql
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM (
    SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, SUM(wl_extendedprice * (1 - wl_discount)) AS total_revenue
    FROM supplier s
    JOIN web_lineitem wl ON s.s_suppkey = wl.wl_suppkey
    WHERE wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY s.s_suppkey, s.s_name, s.s_address, s.s_phone

    UNION ALL

    SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, SUM(sl_extendedprice * (1 - sl_discount)) AS total_revenue
    FROM supplier s
    JOIN store_lineitem sl ON s.s_suppkey = sl.sl_suppkey
    WHERE sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY s.s_suppkey, s.s_name, s.s_address, s.s_phone
) AS supplier_revenue
GROUP BY s_suppkey, s_name, s_address, s_phone, total_revenue
HAVING total_revenue = (
    SELECT MAX(total_revenue)
    FROM (
        SELECT s.s_suppkey, SUM(wl_extendedprice * (1 - wl_discount)) AS total_revenue
        FROM supplier s
        JOIN web_lineitem wl ON s.s_suppkey = wl.wl_suppkey
        WHERE wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
        GROUP BY s.s_suppkey

        UNION ALL

        SELECT s.s_suppkey, SUM(sl_extendedprice * (1 - sl_discount)) AS total_revenue
        FROM supplier s
        JOIN store_lineitem sl ON s.s_suppkey = sl.sl_suppkey
        WHERE sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
        GROUP BY s.s_suppkey
    ) AS max_revenue
)
ORDER BY s_suppkey;
```

Token count = 2232

