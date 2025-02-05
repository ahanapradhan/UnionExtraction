```sql
SELECT SUM(revenue_increase) AS total_revenue_increase
FROM (
    SELECT wl_extendedprice * wl_discount AS revenue_increase
    FROM web_lineitem
    WHERE wl_shipdate >= DATE '1993-01-01'
      AND wl_shipdate < DATE '1995-01-01'
      AND wl_discount BETWEEN 0.05 AND 0.07
      AND wl_quantity < 24
    UNION ALL
    SELECT sl_extendedprice * sl_discount AS revenue_increase
    FROM store_lineitem
    WHERE sl_shipdate >= DATE '1993-01-01'
      AND sl_shipdate < DATE '1995-01-01'
      AND sl_discount BETWEEN 0.05 AND 0.07
      AND sl_quantity < 24
) AS combined_lineitems;
```

Token count = 2067

