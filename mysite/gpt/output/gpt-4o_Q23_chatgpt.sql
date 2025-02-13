```sql
SELECT   RIGHT(c_address, 5) AS city,
         p_brand             AS part_brand
FROM     customer,
         orders o1,
         orders o2,
         store_lineitem,
         web_lineitem,
         part
WHERE    c_custkey = o1.o_custkey
AND      c_custkey = o2.o_custkey 
AND      o1.o_orderkey = wl_orderkey
AND      wl_returnflag = 'A' 
AND      o2.o_orderkey = sl_orderkey
AND      sl_returnflag = 'N' 
AND      wl_partkey = sl_partkey
AND      sl_partkey = p_partkey
AND      o1.o_orderdate < o2.o_orderdate
AND      wl_receiptdate < sl_receiptdate 
AND      o1.o_orderdate BETWEEN date '1995-01-01' AND date '1995-12-31'
AND      o2.o_orderdate BETWEEN date '1995-01-01' AND date '1995-12-31'
GROUP BY RIGHT(c_address, 5),
         p_brand 
ORDER BY city, part_brand;
```

-- Prompt Token count = 1911

