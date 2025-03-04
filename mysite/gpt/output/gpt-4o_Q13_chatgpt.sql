```sql
SELECT 
    c_count, 
    COUNT(*) AS c_orderdate, 
    custdist 
FROM (
    SELECT 
        c.c_custkey, 
        COUNT(o.o_orderkey) AS c_count 
    FROM 
        customer c 
    LEFT JOIN 
        orders o 
    ON 
        c.c_custkey = o.o_custkey 
        AND o.o_comment NOT LIKE '%special%requests%' 
    GROUP BY 
        c.c_custkey
) AS customer_orders 
GROUP BY 
    c_count 
ORDER BY 
    c_count DESC;
```

-- Prompt Token count = 4584

```sql
SELECT 
    subquery.c_count, 
    COUNT(*) AS c_orderdate, 
    subquery.custdist 
FROM 
    (
        SELECT 
            o.o_orderdate AS c_count, 
            COUNT(o.o_orderkey) AS c_orderdate, 
            c.c_custkey
        FROM 
            customer c 
        LEFT JOIN 
            orders o 
        ON 
            c.c_custkey = o.o_custkey 
            AND o.o_comment NOT LIKE '%special%requests%' 
        GROUP BY 
            o.o_orderdate, c.c_custkey
    ) AS subquery 
GROUP BY 
    subquery.c_count, subquery.custdist 
ORDER BY 
    subquery.c_count DESC, subquery.custdist;
```

-- Prompt Token count = 6069

I don't know anymore.

-- Prompt Token count = 6091

