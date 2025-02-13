```sql
SELECT ps_partkey, SUM(ps_availqty * ps_supplycost) AS part_value
FROM nation, supplier, partsupp
WHERE supplier.s_nationkey = nation.n_nationkey 
AND supplier.s_suppkey = partsupp.ps_suppkey 
AND nation.n_name = 'INDIA'
GROUP BY ps_partkey
HAVING SUM(ps_availqty * ps_supplycost) >= 0.00001 * (SELECT SUM(ps_availqty * ps_supplycost) FROM nation, supplier, partsupp WHERE supplier.s_nationkey = nation.n_nationkey AND supplier.s_suppkey = partsupp.ps_suppkey AND nation.n_name = 'INDIA')
ORDER BY part_value DESC;
```

-- Prompt Token count = 434

```sql
SELECT ps_partkey, part_value
FROM (
    SELECT partsupp.ps_partkey, 
           SUM(partsupp.ps_availqty * partsupp.ps_supplycost) AS part_value
    FROM nation, supplier, partsupp
    WHERE supplier.s_nationkey = nation.n_nationkey 
      AND supplier.s_suppkey = partsupp.ps_suppkey 
      AND nation.n_name = 'INDIA'
    GROUP BY partsupp.ps_partkey
) AS part_values
WHERE part_value >= 0.00001 * (SELECT SUM(ps_availqty * ps_supplycost) 
                               FROM nation, supplier, partsupp
                               WHERE supplier.s_nationkey = nation.n_nationkey 
                                 AND supplier.s_suppkey = partsupp.ps_suppkey 
                                 AND nation.n_name = 'INDIA')
ORDER BY part_value DESC;
```

-- Prompt Token count = 482

