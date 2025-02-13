```sql
SELECT c_custkey, c_name, SUM(revenue) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
FROM (
    SELECT c_custkey, c_name, sl_extendedprice * (1 - sl_discount) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, nation, orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND customer.c_nationkey = nation.n_nationkey
    AND customer.c_custkey = orders.o_custkey
    AND store_lineitem.sl_returnflag = 'R'
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    UNION ALL
    SELECT c_custkey, c_name, wl_extendedprice * (1 - wl_discount) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, nation, orders, web_lineitem
    WHERE customer.c_nationkey = nation.n_nationkey
    AND orders.o_orderkey = web_lineitem.wl_orderkey
    AND customer.c_custkey = orders.o_custkey
    AND web_lineitem.wl_returnflag = 'R'
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined
GROUP BY c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment
ORDER BY revenue DESC, c_custkey ASC, c_name ASC, c_acctbal ASC, c_phone ASC, n_name ASC, c_address ASC, c_comment ASC
LIMIT 20;
```

-- Prompt Token count = 2157

