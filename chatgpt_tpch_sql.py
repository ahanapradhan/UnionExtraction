```sql
SELECT 
    n.n_name AS nation, 
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year, 
    SUM((wl.wl_extendedprice * (1 - wl.wl_discount)) - (ps.ps_supplycost * wl.wl_quantity)) AS sum_profit
FROM 
    nation n
JOIN 
    supplier s ON n.n_nationkey = s.s_nationkey
JOIN 
    partsupp ps ON s.s_suppkey = ps.ps_suppkey
JOIN 
    part p ON ps.ps_partkey = p.p_partkey
JOIN 
    web_lineitem wl ON p.p_partkey = wl.wl_partkey AND ps.ps_suppkey = wl.wl_suppkey
JOIN 
    orders o ON wl.wl_orderkey = o.o_orderkey
WHERE 
    p.p_name LIKE '%specified_substring%'
GROUP BY 
    n.n_name, o_year

UNION ALL

SELECT 
    n.n_name AS nation, 
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year, 
    SUM((sl.sl_extendedprice * (1 - sl.sl_discount)) - (ps.ps_supplycost * sl.sl_quantity)) AS sum_profit
FROM 
    nation n
JOIN 
    supplier s ON n.n_nationkey = s.s_nationkey
JOIN 
    partsupp ps ON s.s_suppkey = ps.ps_suppkey
JOIN 
    part p ON ps.ps_partkey = p.p_partkey
JOIN 
    store_lineitem sl ON p.p_partkey = sl.sl_partkey AND ps.ps_suppkey = sl.sl_suppkey
JOIN 
    orders o ON sl.sl_orderkey = o.o_orderkey
WHERE 
    p.p_name LIKE '%specified_substring%'
GROUP BY 
    n.n_name, o_year

ORDER BY 
    nation ASC, o_year DESC;
```

Token count = 1955

