```sql
SELECT 
    SUBSTRING(c1.c_phone FROM 1 FOR 2) AS cntrycode, 
    COUNT(DISTINCT c1.c_custkey) AS numcust, 
    SUM(c1.c_acctbal) AS totacctbal
FROM 
    customer c1
LEFT JOIN 
    orders o ON c1.c_custkey = o.o_custkey AND o.o_orderdate >= CURRENT_DATE - INTERVAL '7 years'
WHERE 
    SUBSTRING(c1.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c1.c_acctbal > (
        SELECT AVG(c2.c_acctbal)
        FROM customer c2
        WHERE c2.c_acctbal > 0
    )
    AND o.o_orderkey IS NULL
GROUP BY 
    cntrycode
ORDER BY 
    cntrycode;
```

-- Prompt Token count = 939

```sql
SELECT 
    SUBSTRING(c1.c_phone FROM 1 FOR 2) AS cntrycode, 
    COUNT(DISTINCT c1.c_custkey) AS numcust, 
    SUM(c1.c_acctbal) AS totacctbal
FROM 
    customer c1
LEFT JOIN 
    orders o ON c1.c_custkey = o.o_custkey
WHERE 
    SUBSTRING(c1.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c1.c_acctbal > (
        SELECT AVG(c2.c_acctbal)
        FROM customer c2
        WHERE c2.c_acctbal > 0
          AND SUBSTRING(c2.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    )
    AND o.o_orderkey IS NULL
GROUP BY 
    cntrycode
ORDER BY 
    cntrycode;
```

-- Prompt Token count = 1261

