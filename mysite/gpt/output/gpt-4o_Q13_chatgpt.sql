```
SELECT 
    subquery.c_count, 
    COUNT(*) AS c_orderdate, 
    subquery.custdist 
FROM 
    (
        SELECT 
            o.o_orderdate AS c_count, 
            COUNT(o.o_orderkey) AS custdist 
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
    subquery.c_count DESC, subquery.custdist ASC;
```

-- Prompt Token count = 4984

```sql
SELECT 
    subquery.c_count, 
    COUNT(*) AS c_orderdate, 
    subquery.custdist 
FROM 
    (
        SELECT 
            o.o_orderdate AS c_count, 
            COUNT(o.o_orderkey) AS custdist 
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

-- Prompt Token count = 6480

```sql
SELECT 
    subquery.c_count, 
    COUNT(*) AS c_orderdate, 
    subquery.custdist 
FROM 
    (
        SELECT 
            o.o_orderdate AS c_count, 
            COUNT(o.o_orderkey) AS custdist 
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
    subquery.c_count DESC, subquery.custdist DESC;
```

-- Prompt Token count = 6691

