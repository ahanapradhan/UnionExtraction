```sql
SELECT s_name, s_address
FROM supplier
JOIN partsupp ON supplier.s_suppkey = partsupp.ps_suppkey
JOIN part ON partsupp.ps_partkey = part.p_partkey
JOIN nation ON supplier.s_nationkey = nation.n_nationkey
WHERE nation.n_name = 'FRANCE'
AND part.p_name LIKE '%ivory%'
AND partsupp.ps_availqty > (
    SELECT 0.5 * SUM(wl_quantity)
    FROM web_lineitem
    WHERE wl_partkey = part.p_partkey
    AND wl_suppkey = supplier.s_suppkey
    AND wl_shipdate BETWEEN '1995-01-01' AND '1995-12-31'
)
ORDER BY s_name ASC;
```

-- Prompt Token count = 5801

