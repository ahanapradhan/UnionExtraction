```sql
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM nation, part, partsupp, region, supplier
WHERE nation.n_nationkey = supplier.s_nationkey
  AND nation.n_regionkey = region.r_regionkey
  AND part.p_partkey = partsupp.ps_partkey
  AND partsupp.ps_suppkey = supplier.s_suppkey
  AND part.p_size = 15
  AND region.r_name = 'EUROPE'
  AND part.p_type LIKE '%BRASS'
  AND partsupp.ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp ps
    WHERE ps.ps_partkey = part.p_partkey
  )
ORDER BY s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
LIMIT 100;
```

-- Prompt Token count = 9669

