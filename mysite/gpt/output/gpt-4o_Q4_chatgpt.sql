```sql
SELECT o_orderpriority, SUM(order_count) AS order_count
FROM (
    SELECT o_orderpriority, COUNT(DISTINCT orders.o_orderkey) AS order_count
    FROM orders, web_lineitem
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
    UNION ALL
    SELECT o_orderpriority, COUNT(DISTINCT orders.o_orderkey) AS order_count
    FROM orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 656

```sql
SELECT o_orderpriority, COUNT(DISTINCT orders.o_orderkey) AS order_count
FROM orders, web_lineitem
WHERE orders.o_orderkey = web_lineitem.wl_orderkey
AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
GROUP BY o_orderpriority
UNION ALL
SELECT o_orderpriority, COUNT(DISTINCT orders.o_orderkey) AS order_count
FROM orders, store_lineitem
WHERE orders.o_orderkey = store_lineitem.sl_orderkey
AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 703

