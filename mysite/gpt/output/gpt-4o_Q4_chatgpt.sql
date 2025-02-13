```sql
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM (
    SELECT orders.o_orderpriority, orders.o_orderkey
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    UNION
    SELECT orders.o_orderpriority, orders.o_orderkey
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 967

```sql
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM (
    SELECT orders.o_orderpriority, orders.o_orderkey
    FROM orders, web_lineitem
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    UNION
    SELECT orders.o_orderpriority, orders.o_orderkey
    FROM orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 998

```sql
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM (
    SELECT orders.o_orderpriority, orders.o_orderkey
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    UNION
    SELECT orders.o_orderpriority, orders.o_orderkey
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 1311

