```sql
SELECT partsupp.ps_partkey, nation.n_name, SUM(partsupp.ps_availqty * partsupp.ps_supplycost) AS part_value
FROM nation, supplier, partsupp
WHERE supplier.s_nationkey = nation.n_nationkey 
  AND supplier.s_suppkey = partsupp.ps_suppkey 
  AND nation.n_name = 'INDIA'
GROUP BY partsupp.ps_partkey, nation.n_name
HAVING SUM(partsupp.ps_availqty * partsupp.ps_supplycost) >= 
       (SELECT SUM(ps_availqty * ps_supplycost) * 0.00001 
        FROM partsupp, supplier, nation 
        WHERE supplier.s_suppkey = partsupp.ps_suppkey 
          AND supplier.s_nationkey = nation.n_nationkey 
          AND nation.n_name = 'INDIA')
ORDER BY part_value DESC;
```

Token count = 2124

