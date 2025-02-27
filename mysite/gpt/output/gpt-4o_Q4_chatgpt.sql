```sql
SELECT o_orderpriority, SUM(order_count) AS order_count
FROM (
    SELECT o_orderpriority, COUNT(*) AS order_count
    FROM orders, web_lineitem
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
    UNION ALL
    SELECT o_orderpriority, COUNT(*) AS order_count
    FROM orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
) AS combined_orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 2056

```sql
SELECT o_orderpriority, SUM(order_count) as order_count
FROM (
    SELECT o_orderpriority, COUNT(*) as order_count
    FROM orders, web_lineitem
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
    UNION ALL
    SELECT o_orderpriority, COUNT(*) as order_count
    FROM orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 2087

```sql
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM (
    SELECT orders.o_orderkey, o_orderpriority
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    UNION
    SELECT orders.o_orderkey, o_orderpriority
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 2400

