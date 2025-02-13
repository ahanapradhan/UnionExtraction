```sql
SELECT RIGHT(c_address, 5) AS city
FROM   customer,
       orders o1,
       orders o2,
       store_lineitem,
       web_lineitem w,
       part,
       web_lineitem w1,
       partsupp ps1,
       partsupp ps2
WHERE  c_custkey = o1.o_custkey
       AND c_custkey = o2.o_custkey
       AND o1.o_orderkey = sl_orderkey
       AND sl_returnflag = 'A'
       AND o2.o_orderkey = w.wl_orderkey
       AND w.wl_returnflag = 'N'
       AND w.wl_partkey = sl_partkey
       AND sl_partkey = p_partkey
       AND w1.wl_partkey = p_partkey
       AND sl_receiptdate < w.wl_receiptdate
       AND o1.o_orderdate < o2.o_orderdate
       AND w.wl_suppkey = ps1.ps_suppkey
       AND w1.wl_suppkey = ps2.ps_suppkey
       AND ps2.ps_availqty >= ps1.ps_availqty
       AND o1.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
       AND o2.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
GROUP  BY RIGHT(c_address, 5);
```

-- Prompt Token count = 2061

