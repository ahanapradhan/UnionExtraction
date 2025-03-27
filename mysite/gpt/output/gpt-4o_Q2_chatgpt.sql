```sql
SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment
FROM part p
JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
JOIN region r ON n.n_regionkey = r.r_regionkey
WHERE p.p_size = 15
AND p.p_type LIKE '%BRASS'
AND r.r_name = 'EUROPE'
AND ps.ps_supplycost = (
    SELECT MIN(ps2.ps_supplycost)
    FROM partsupp ps2
    WHERE ps2.ps_partkey = p.p_partkey
)
ORDER BY s.s_acctbal DESC, n.n_name ASC, s.s_name ASC, p.p_partkey ASC
LIMIT 100;
```

-- Prompt Token count = 9547

