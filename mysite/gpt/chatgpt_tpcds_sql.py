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
