```sql
SELECT p_brand, p_type, p_size, COUNT(*) AS supplier_cnt
FROM part
JOIN partsupp ON part.p_partkey = partsupp.ps_partkey
JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey
WHERE part.p_size IN (1, 4, 7)
AND part.p_brand <> 'Brand#23'
AND part.p_type NOT LIKE 'MEDIUM POLISHED%'
AND supplier.s_comment NOT LIKE '%Customer Complaints%'
GROUP BY p_brand, p_size, p_type
ORDER BY supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC;
```

-- Prompt Token count = 1845

