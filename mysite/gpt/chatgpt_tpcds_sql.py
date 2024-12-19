gpt_Q2_subquery = """SELECT
   ws_sold_date_sk AS sold_date_sk,
   'web' AS sales_channel,
   ws_ext_sales_price AS sales_price
FROM
   web_sales
UNION ALL
SELECT
   cs_sold_date_sk AS sold_date_sk,
   'catalog' AS sales_channel,
   cs_ext_sales_price AS sales_price
FROM
   catalog_sales;
"""

gpt_Q4_CTE = """SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    'store' AS sales_type,
    SUM(ss_net_paid) AS total_sales
FROM
    customer
JOIN
    store_sales ON c_customer_id = ss_customer_sk
GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address

UNION ALL

SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    'catalog' AS sales_type,
    SUM(cs_net_paid) AS total_sales
FROM
    customer
JOIN
    catalog_sales ON c_customer_id = cs_bill_customer_sk
GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address

UNION ALL

SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    'web' AS sales_type,
    SUM(ws_net_paid) AS total_sales
FROM
    customer
JOIN
    web_sales ON c_customer_id = ws_bill_customer_sk
GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address;"""

gpt_Q5_CTE = """SELECT
   ws_web_site_id,
   SUM(ws_ext_sales_price) AS total_sales_revenue,
   SUM(ws_net_profit) AS total_sales_profit,
   SUM(COALESCE(wr_return_amt, 0)) AS total_return_amount,
   SUM(ws_ext_sales_price) - SUM(COALESCE(wr_return_amt, 0)) AS net_loss
FROM
   web_sales
LEFT JOIN
   web_returns
   ON ws_order_number = wr_order_number
   AND ws_item_sk = wr_item_sk
JOIN
   date_dim
   ON ws_sold_date_sk = d_date_sk
WHERE
   d_date BETWEEN 'start_date' AND 'end_date'
GROUP BY
   ws_web_site_id;
"""

gpt_Q11_CTE = """SELECT
   c.customer_id,
   c.first_name,
   c.last_name,
   c.preferred_cust_flag,
   c.birth_country,
   c.customer_login,
   c.email_address,
   COALESCE(SUM(ss.net_paid), 0) AS total_store_sales,
   COALESCE(SUM(ws.web_net_paid), 0) AS total_web_sales
FROM
   customer c
LEFT JOIN
   store_sales ss
   ON c.customer_id = ss.customer_id
LEFT JOIN
   web_sales ws
   ON c.customer_id = ws.customer_id
GROUP BY
   c.customer_id,
   c.first_name,
   c.last_name,
   c.preferred_cust_flag,
   c.birth_country,
   c.customer_login,
   c.email_address;
"""

gpt_Q33_subquery = """SELECT
   i_manufact_id,
   SUM(CASE WHEN d_month = 3 AND d_year = 1999 THEN ss_sales_price ELSE 0 END) AS store_sales_revenue,
   SUM(CASE WHEN d_month = 3 AND d_year = 1999 THEN cs_sales_price ELSE 0 END) AS catalog_sales_revenue,
   SUM(CASE WHEN d_month = 3 AND d_year = 1999 THEN ws_sales_price ELSE 0 END) AS web_sales_revenue
FROM
   store_sales
JOIN
   item ON store_sales.ss_item_sk = item.i_item_sk
JOIN
   date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
JOIN
   customer ON store_sales.ss_customer_sk = customer.c_customer_sk
JOIN
   catalog_sales ON catalog_sales.cs_item_sk = item.i_item_sk
JOIN
   date_dim AS catalog_date ON catalog_sales.cs_sold_date_sk = catalog_date.d_date_sk
JOIN
   customer AS catalog_customer ON catalog_sales.cs_bill_customer_sk = catalog_customer.c_customer_sk
JOIN
   web_sales ON web_sales.ws_item_sk = item.i_item_sk
JOIN
   date_dim AS web_date ON web_sales.ws_sold_date_sk = web_date.d_date_sk
JOIN
   customer AS web_customer ON web_sales.ws_bill_customer_sk = web_customer.c_customer_sk
WHERE
   i_category = 'Books'
   AND customer.c_current_cdemo_sk = catalog_customer.c_current_cdemo_sk
   AND customer.c_current_cdemo_sk = web_customer.c_current_cdemo_sk
   AND customer.c_current_addr_sk = catalog_customer.c_current_addr_sk
   AND customer.c_current_addr_sk = web_customer.c_current_addr_sk
   AND customer.c_current_addr_sk IN (
       SELECT ca_address_sk
       FROM customer_address
       WHERE ca_gmt_offset = -5
   )
GROUP BY
   i_manufact_id;
"""

gpt_Q54_subquery = """SELECT
   c.customer_id,
   c.customer_first_name,
   c.customer_last_name,
   i.item_id,
   i.item_desc,
   COALESCE(cs.cs_quantity, 0) AS catalog_sales_quantity,
   COALESCE(ws.ws_quantity, 0) AS web_sales_quantity,
   COALESCE(cs.cs_sales_price, 0) AS catalog_sales_amount,
   COALESCE(ws.ws_sales_price, 0) AS web_sales_amount,
   (COALESCE(cs.cs_quantity, 0) + COALESCE(ws.ws_quantity, 0)) AS total_quantity,
   (COALESCE(cs.cs_sales_price, 0) + COALESCE(ws.ws_sales_price, 0)) AS total_sales_amount
FROM
   customer c
LEFT JOIN
   catalog_sales cs ON c.customer_id = cs.cs_bill_customer_id
LEFT JOIN
   web_sales ws ON c.customer_id = ws.ws_bill_customer_id
LEFT JOIN
   item i ON cs.cs_item_sk = i.item_sk OR ws.ws_item_sk = i.item_sk
WHERE
   cs.cs_quantity IS NOT NULL OR ws.ws_quantity IS NOT NULL
ORDER BY
   c.customer_id,
   i.item_id;
"""

