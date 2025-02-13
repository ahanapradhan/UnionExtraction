```sql
SELECT 
    EXTRACT(YEAR FROM o_orderdate) AS o_year, 
    SUM(CASE WHEN n2.n_name = 'INDIA' THEN wl_extendedprice * (1 - wl_discount) ELSE 0 END) / SUM(wl_extendedprice * (1 - wl_discount)) AS mkt_share
FROM 
    customer, web_lineitem, nation n1, nation n2, orders, part, region, supplier
WHERE 
    c_custkey = o_custkey
    AND c_nationkey = n1.n_nationkey
    AND wl_orderkey = o_orderkey
    AND wl_partkey = p_partkey
    AND wl_suppkey = s_suppkey
    AND n1.n_regionkey = r_regionkey
    AND n2.n_nationkey = s_nationkey
    AND p_type = 'ECONOMY ANODIZED STEEL'
    AND r_name = 'ASIA'
    AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY 
    EXTRACT(YEAR FROM o_orderdate)
ORDER BY 
    o_year;
```

-- Prompt Token count = 825

```sql
SELECT 
    EXTRACT(YEAR FROM o_orderdate) AS o_year, 
    SUM(CASE WHEN n2.n_name = 'INDIA' THEN wl_extendedprice * (1 - wl_discount) ELSE 0 END) / SUM(wl_extendedprice * (1 - wl_discount)) AS mkt_share
FROM 
    customer, web_lineitem, nation n1, nation n2, orders, part, region, supplier
WHERE 
    c_custkey = o_custkey
    AND c_nationkey = n1.n_nationkey
    AND wl_orderkey = o_orderkey
    AND wl_partkey = p_partkey
    AND wl_suppkey = s_suppkey
    AND n1.n_regionkey = r_regionkey
    AND n2.n_nationkey = s_nationkey
    AND p_type = 'ECONOMY ANODIZED STEEL'
    AND r_name = 'ASIA'
    AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY 
    EXTRACT(YEAR FROM o_orderdate)
ORDER BY 
    o_year;
```

-- Prompt Token count = 1135

