```sql
SELECT shipmode, 
       SUM(high_line_count) AS high_line_count, 
       SUM(low_line_count) AS low_line_count
FROM (
    SELECT sl_shipmode AS shipmode, 
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
    FROM orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
      AND store_lineitem.sl_shipdate < store_lineitem.sl_commitdate
      AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
      AND store_lineitem.sl_shipmode IN ('SHIP', 'TRUCK')
      AND store_lineitem.sl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY sl_shipmode
    UNION ALL
    SELECT wl_shipmode AS shipmode, 
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
    FROM orders, web_lineitem
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
      AND web_lineitem.wl_shipdate < web_lineitem.wl_commitdate
      AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
      AND web_lineitem.wl_shipmode IN ('SHIP', 'TRUCK')
      AND web_lineitem.wl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY wl_shipmode
) AS combined
GROUP BY shipmode;
```

-- Prompt Token count = 1109

```sql
SELECT shipmode, 
       SUM(high_line_count) AS high_line_count, 
       SUM(low_line_count) AS low_line_count
FROM (
    SELECT sl_shipmode AS shipmode, 
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count 
    FROM orders, store_lineitem 
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
      AND store_lineitem.sl_shipdate < store_lineitem.sl_commitdate
      AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
      AND store_lineitem.sl_shipmode IN ('SHIP', 'TRUCK')
      AND store_lineitem.sl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY sl_shipmode
    UNION ALL  
    SELECT wl_shipmode AS shipmode, 
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count 
    FROM orders, web_lineitem 
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
      AND web_lineitem.wl_shipdate < web_lineitem.wl_commitdate
      AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
      AND web_lineitem.wl_shipmode IN ('SHIP', 'TRUCK')
      AND web_lineitem.wl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY wl_shipmode
) AS combined
GROUP BY shipmode;
```

-- Prompt Token count = 1556

