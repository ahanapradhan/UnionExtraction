```sql
SELECT s_name, s_address
FROM nation, part, partsupp, supplier
  WHERE part.p_partkey = partsupp.ps_partkey
  AND partsupp.ps_suppkey = supplier.s_suppkey
  AND nation.n_nationkey = supplier.s_nationkey
  AND nation.n_name = 'FRANCE'
  AND part.p_name LIKE '%ivory%'
  AND partsupp.ps_availqty > 0.5 * (
    SELECT SUM(wl_quantity)
    FROM web_lineitem wl
    WHERE wl.wl_partkey = part.p_partkey
      AND wl.wl_suppkey = supplier.s_suppkey
      AND wl.wl_shipdate BETWEEN '1995-01-01' AND '1995-12-31'
  )
ORDER BY s_name ASC;
```

-- Prompt Token count = 5771

