```sql
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM orders
JOIN (
    SELECT wl_orderkey AS orderkey
    FROM web_lineitem
    WHERE wl_receiptdate > wl_commitdate
    UNION
    SELECT sl_orderkey AS orderkey
    FROM store_lineitem
    WHERE sl_receiptdate > sl_commitdate
) AS delayed_orders ON orders.o_orderkey = delayed_orders.orderkey
WHERE EXTRACT(YEAR FROM o_orderdate) = 1995
AND EXTRACT(QUARTER FROM o_orderdate) = <quarter_number>
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

-- Prompt Token count = 1560

```sql
SELECT 
    o_orderpriority, 
    COUNT(DISTINCT o_orderkey) AS order_count
FROM 
    orders
JOIN 
    (SELECT wl_orderkey AS orderkey
     FROM web_lineitem
     WHERE wl_receiptdate > wl_commitdate
     UNION
     SELECT sl_orderkey AS orderkey
     FROM store_lineitem
     WHERE sl_receiptdate > sl_commitdate) AS delayed_orders
ON 
    orders.o_orderkey = delayed_orders.orderkey
WHERE 
    EXTRACT(YEAR FROM o_orderdate) = 1995
    AND EXTRACT(QUARTER FROM o_orderdate) = 1
GROUP BY 
    o_orderpriority
ORDER BY 
    o_orderpriority ASC;
```

-- Prompt Token count = 1591

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

-- Prompt Token count = 1904

