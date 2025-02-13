```sql
SELECT
    SUM(wl_extendedprice * (1 - wl_discount)) AS revenue
FROM
    web_lineitem,
    part
WHERE
    p_partkey = wl_partkey
    AND p_brand = 'Brand#12'
    AND wl_shipinstruct = 'DELIVER IN PERSON'
    AND wl_shipmode = 'AIR'
    AND (
        (p_size BETWEEN 1 AND 5 AND wl_quantity BETWEEN 1 AND 11 AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))
        OR (p_size BETWEEN 1 AND 10 AND wl_quantity BETWEEN 10 AND 20 AND p_container IN ('MED CASE', 'MED BOX', 'MED PACK', 'MED PKG'))
        OR (p_size BETWEEN 1 AND 15 AND wl_quantity BETWEEN 20 AND 30 AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))
    );
```

-- Prompt Token count = 2636

