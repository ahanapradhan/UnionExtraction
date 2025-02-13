```sql
SELECT s_name, COUNT(*) AS numwait
FROM supplier, nation, web_lineitem l1, web_lineitem l2, orders
WHERE l1.wl_orderkey = l2.wl_orderkey
  AND l2.wl_orderkey = o_orderkey
  AND l1.wl_suppkey = s_suppkey
  AND n_nationkey = s_nationkey
  AND l1.wl_commitdate < l1.wl_receiptdate
  AND n_name = 'ARGENTINA'
  AND o_orderstatus = 'F'
  AND l1.wl_suppkey <> l2.wl_suppkey
  AND NOT EXISTS (
    SELECT 1
    FROM web_lineitem l3
    WHERE l3.wl_orderkey = l1.wl_orderkey
      AND l3.wl_suppkey <> l1.wl_suppkey
      AND l3.wl_commitdate < l3.wl_receiptdate
  )
GROUP BY s_name
ORDER BY numwait DESC, s_name ASC;
```

-- Prompt Token count = 5091

```sql
SELECT s_name, COUNT(*) AS numwait
FROM supplier, nation, orders, web_lineitem l1
WHERE s_suppkey = l1.wl_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'ARGENTINA'
  AND l1.wl_orderkey = o_orderkey
  AND o_orderstatus = 'F'
  AND l1.wl_commitdate < l1.wl_receiptdate
  AND EXISTS (
    SELECT 1
    FROM web_lineitem l2
    WHERE l1.wl_orderkey = l2.wl_orderkey
      AND l1.wl_suppkey <> l2.wl_suppkey
  )
  AND NOT EXISTS (
    SELECT 1
    FROM web_lineitem l3
    WHERE l1.wl_orderkey = l3.wl_orderkey
      AND l1.wl_suppkey <> l3.wl_suppkey
      AND l3.wl_commitdate < l3.wl_receiptdate
  )
GROUP BY s_name
ORDER BY numwait DESC, s_name;
```

-- Prompt Token count = 7528

