```sql
SELECT 
    SUM(promo_revenue) / SUM(total_revenue) * 100 AS promo_revenue_percentage
FROM (
    SELECT 
        SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN store_lineitem.sl_extendedprice * (1 - store_lineitem.sl_discount) ELSE 0 END) AS promo_revenue,
        SUM(store_lineitem.sl_extendedprice * (1 - store_lineitem.sl_discount)) AS total_revenue
    FROM 
        part, store_lineitem
    WHERE 
        part.p_partkey = store_lineitem.sl_partkey
        AND store_lineitem.sl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
    UNION ALL
    SELECT 
        SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN web_lineitem.wl_extendedprice * (1 - web_lineitem.wl_discount) ELSE 0 END) AS promo_revenue,
        SUM(web_lineitem.wl_extendedprice * (1 - web_lineitem.wl_discount)) AS total_revenue
    FROM 
        part, web_lineitem
    WHERE 
        part.p_partkey = web_lineitem.wl_partkey
        AND web_lineitem.wl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
) AS combined_revenue
```


-- Prompt Token count = 484

```sql
SELECT SUM(promo_revenue) / SUM(total_revenue) * 100 AS promo_revenue_percentage
FROM (
    SELECT 
        SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN store_lineitem.sl_extendedprice * (1 - store_lineitem.sl_discount) ELSE 0 END) AS promo_revenue,
        SUM(store_lineitem.sl_extendedprice * (1 - store_lineitem.sl_discount)) AS total_revenue
    FROM 
        part, store_lineitem
    WHERE 
        part.p_partkey = store_lineitem.sl_partkey
        AND store_lineitem.sl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
    UNION ALL
    SELECT 
        SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN web_lineitem.wl_extendedprice * (1 - web_lineitem.wl_discount) ELSE 0 END) AS promo_revenue,
        SUM(web_lineitem.wl_extendedprice * (1 - web_lineitem.wl_discount)) AS total_revenue
    FROM 
        part, web_lineitem
    WHERE 
        part.p_partkey = web_lineitem.wl_partkey
        AND web_lineitem.wl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
) AS combined_revenue;
```

-- Prompt Token count = 554

