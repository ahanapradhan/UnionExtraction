```sql
SELECT 
    nation, 
    EXTRACT(YEAR FROM o_orderdate) AS o_year, 
    SUM(profit) AS sum_profit
FROM (
    SELECT 
        n_name AS nation, 
        o_orderdate, 
        (-ps_supplycost * wl_quantity + wl_extendedprice * (1 - wl_discount)) AS profit
    FROM 
        nation, orders, part, partsupp, supplier, web_lineitem
    WHERE 
        orders.o_orderkey = web_lineitem.wl_orderkey
        AND part.p_partkey = partsupp.ps_partkey
        AND partsupp.ps_partkey = web_lineitem.wl_partkey
        AND partsupp.ps_suppkey = supplier.s_suppkey
        AND supplier.s_suppkey = web_lineitem.wl_suppkey
        AND nation.n_nationkey = supplier.s_nationkey
        AND part.p_name LIKE '%co%'
    UNION ALL
    SELECT 
        n_name AS nation, 
        o_orderdate, 
        (-ps_supplycost * sl_quantity + sl_extendedprice * (1 - sl_discount)) AS profit
    FROM 
        nation, orders, part, partsupp, store_lineitem, supplier
    WHERE 
        orders.o_orderkey = store_lineitem.sl_orderkey
        AND part.p_partkey = partsupp.ps_partkey
        AND partsupp.ps_partkey = store_lineitem.sl_partkey
        AND partsupp.ps_suppkey = store_lineitem.sl_suppkey
        AND store_lineitem.sl_suppkey = supplier.s_suppkey
        AND nation.n_nationkey = supplier.s_nationkey
        AND part.p_name LIKE '%co%'
) AS combined
GROUP BY 
    nation, o_year
ORDER BY 
    nation ASC, o_year DESC;
```

-- Prompt Token count = 2106

