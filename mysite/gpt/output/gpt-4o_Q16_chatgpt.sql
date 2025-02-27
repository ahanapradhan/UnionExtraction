```sql
SELECT p_brand, p_type, p_size, COUNT(*) AS supplier_cnt
FROM part, partsupp, supplier
WHERE part.p_partkey = partsupp.ps_partkey
  AND part.p_size IN (1, 4, 7)
  AND part.p_brand <> 'Brand#23'
  AND part.p_type NOT LIKE 'MEDIUM POLISHED%'
  AND supplier.s_comment NOT LIKE '%Customer Complaints%'
GROUP BY p_brand, p_size, p_type
ORDER BY supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC;
```

-- Prompt Token count = 1865

```sql
SELECT p_brand, p_type, p_size, COUNT(distinct s_suppkey) AS supplier_cnt
FROM part, partsupp, supplier
WHERE part.p_partkey = partsupp.ps_partkey
  AND part.p_size IN (1, 4, 7)
  AND part.p_brand <> 'Brand#23'
  AND part.p_type NOT LIKE 'MEDIUM POLISHED%'
  AND partsupp.ps_suppkey = supplier.s_suppkey
  AND supplier.s_comment NOT LIKE '%Customer Complaints%'
GROUP BY p_brand, p_size, p_type
ORDER BY supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC;
```

-- Prompt Token count = 2088

