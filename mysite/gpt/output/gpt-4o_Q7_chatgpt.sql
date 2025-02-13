```sql
SELECT 
    supp_nation, 
    cust_nation, 
    EXTRACT(YEAR FROM l_year) AS l_year, 
    SUM(revenue) AS revenue
FROM (
    SELECT 
        n1.n_name AS supp_nation, 
        n2.n_name AS cust_nation, 
        wl_shipdate AS l_year, 
        wl_extendedprice * (1 - wl_discount) AS revenue
    FROM 
        customer, 
        nation n1, 
        nation n2, 
        orders, 
        supplier, 
        web_lineitem
    WHERE 
        orders.o_orderkey = web_lineitem.wl_orderkey
        AND supplier.s_suppkey = web_lineitem.wl_suppkey
        AND customer.c_custkey = orders.o_custkey
        AND customer.c_nationkey = n2.n_nationkey
        AND n1.n_nationkey = supplier.s_nationkey
        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' AND n1.n_name = 'GERMANY'))
        AND web_lineitem.wl_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
    UNION ALL
    SELECT 
        n1.n_name AS supp_nation, 
        n2.n_name AS cust_nation, 
        sl_shipdate AS l_year, 
        sl_extendedprice * (1 - sl_discount) AS revenue
    FROM 
        customer, 
        nation n1, 
        nation n2, 
        orders, 
        supplier, 
        store_lineitem
    WHERE 
        orders.o_orderkey = store_lineitem.sl_orderkey
        AND supplier.s_suppkey = store_lineitem.sl_suppkey
        AND customer.c_custkey = orders.o_custkey
        AND customer.c_nationkey = n2.n_nationkey
        AND n1.n_nationkey = supplier.s_nationkey
        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' AND n1.n_name = 'GERMANY'))
        AND store_lineitem.sl_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
) AS combined
GROUP BY 
    supp_nation, 
    cust_nation, 
    EXTRACT(YEAR FROM l_year)
ORDER BY 
    supp_nation, 
    cust_nation, 
    l_year;
```

-- Prompt Token count = 1014

```sql
SELECT supp_nation, cust_nation, l_year, SUM(revenue) as revenue
FROM (
    SELECT n1.n_name as supp_nation, n2.n_name as cust_nation, EXTRACT(YEAR FROM wl_shipdate) as l_year, wl_extendedprice*(1 - wl_discount) as revenue 
    FROM customer, nation n1, nation n2, orders, supplier, web_lineitem 
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND supplier.s_suppkey = web_lineitem.wl_suppkey
    AND customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = n2.n_nationkey
    AND n1.n_nationkey = supplier.s_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' AND n1.n_name = 'GERMANY'))
    AND web_lineitem.wl_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
    UNION ALL  
    SELECT n1.n_name as supp_nation, n2.n_name as cust_nation, EXTRACT(YEAR FROM sl_shipdate) as l_year, sl_extendedprice*(1 - sl_discount) as revenue 
    FROM customer, nation n1, nation n2, orders, supplier, store_lineitem 
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND supplier.s_suppkey = store_lineitem.sl_suppkey
    AND customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = n2.n_nationkey
    AND n1.n_nationkey = supplier.s_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' AND n1.n_name = 'GERMANY'))
    AND store_lineitem.sl_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
) AS combined
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;
```

-- Prompt Token count = 1216

