```sql
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM (
    SELECT o_orderpriority, o_orderkey
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'

    UNION ALL

    SELECT o_orderpriority, o_orderkey
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined_orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;
```

Token count = 2557

