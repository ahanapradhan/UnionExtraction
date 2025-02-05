```sql
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM (
    SELECT supplier.s_suppkey, supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment, partsupp.ps_supplycost
    FROM nation, part, partsupp, region, supplier
    WHERE nation.n_nationkey = supplier.s_nationkey
      AND nation.n_regionkey = region.r_regionkey
      AND part.p_partkey = partsupp.ps_partkey
      AND partsupp.ps_suppkey = supplier.s_suppkey
      AND part.p_size = 15
      AND region.r_name = 'EUROPE'
      AND part.p_type LIKE '%BRASS'
) AS subquery
JOIN (
    SELECT ps_partkey, ps_suppkey, MIN(ps_supplycost) AS min_cost
    FROM partsupp
    JOIN part ON part.p_partkey = partsupp.ps_partkey
    JOIN supplier ON supplier.s_suppkey = partsupp.ps_suppkey
    JOIN nation ON nation.n_nationkey = supplier.s_nationkey
    JOIN region ON region.r_regionkey = nation.n_regionkey
    WHERE part.p_size = 15
      AND region.r_name = 'EUROPE'
      AND part.p_type LIKE '%BRASS'
    GROUP BY ps_partkey, ps_suppkey
) AS min_costs
ON subquery.p_partkey = min_costs.ps_partkey 
AND subquery.s_suppkey = min_costs.ps_suppkey 
AND subquery.ps_supplycost = min_costs.min_cost
ORDER BY s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
LIMIT 100;
```
Token count = 2247

