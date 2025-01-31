```sql
SELECT 
    'store' AS channel, 
    AVG(ss_quantity * ss_list_price) AS avg_sales_value
FROM 
    store_sales
JOIN 
    date_dim ON ss_sold_date_sk = d_date_sk
WHERE 
    d_year BETWEEN 1999 AND 2001

UNION ALL

SELECT 
    'catalog' AS channel, 
    AVG(cs_quantity * cs_list_price) AS avg_sales_value
FROM 
    catalog_sales
JOIN 
    date_dim ON cs_sold_date_sk = d_date_sk
WHERE 
    d_year BETWEEN 1999 AND 2001

UNION ALL

SELECT 
    'web' AS channel, 
    AVG(ws_quantity * ws_list_price) AS avg_sales_value
FROM 
    web_sales
JOIN 
    date_dim ON ws_sold_date_sk = d_date_sk
WHERE 
    d_year BETWEEN 1999 AND 2001;
```
