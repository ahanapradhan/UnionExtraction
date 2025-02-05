```sql
SELECT 
    customer.c_name, 
    customer.c_custkey, 
    orders.o_orderkey, 
    orders.o_orderdate, 
    orders.o_totalprice, 
    SUM(web_lineitem.wl_quantity) AS total_quantity
FROM 
    customer
JOIN 
    orders ON customer.c_custkey = orders.o_custkey
JOIN 
    web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
GROUP BY 
    customer.c_custkey, 
    customer.c_name, 
    orders.o_orderkey, 
    orders.o_orderdate, 
    orders.o_totalprice
HAVING 
    SUM(web_lineitem.wl_quantity) > 300
ORDER BY 
    orders.o_totalprice DESC
LIMIT 100;
```

Token count = 3750

