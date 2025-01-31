Q2_CTE_TEXT = "How can we consolidate and compare the sales data from both web " \
              "and catalog sales channels by their respective sold dates and sales prices?"

Q2_CTE_QH = """SELECT sold_date_sk, 
                sales_price 
         FROM   (SELECT ws_sold_date_sk    sold_date_sk, 
                        ws_ext_sales_price sales_price 
                 FROM   web_sales) as web_sales_date_price
         UNION ALL 
         (SELECT cs_sold_date_sk    sold_date_sk, 
                 cs_ext_sales_price sales_price 
          FROM   catalog_sales)"""

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
"""  # Correct

# ========================================================================================================


Q4_CTE_TEXT = "Identify the total annual sales for each customer, categorized by sales " \
              "type (store, catalog, or web), along with customer details such as name, " \
              "preferred customer status, birth country, login, and email address."

Q4_CTE_QH = """(SELECT c_customer_id                       customer_id, 
                c_first_name                        customer_first_name, 
                c_last_name                         customer_last_name, 
                c_preferred_cust_flag               customer_preferred_cust_flag 
                , 
                c_birth_country 
                customer_birth_country, 
                c_login                             customer_login, 
                c_email_address                     customer_email_address, 
                d_year                              dyear, 
                Sum(( ( ss_ext_list_price - ss_ext_wholesale_cost 
                        - ss_ext_discount_amt 
                      ) 
                      + 
                          ss_ext_sales_price ) / 2) year_total, 
                's'                                 sale_type 
         FROM   customer, 
                store_sales, 
                date_dim 
         WHERE  c_customer_sk = ss_customer_sk 
                AND ss_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year 
         UNION ALL 
         SELECT c_customer_id                             customer_id, 
                c_first_name                              customer_first_name, 
                c_last_name                               customer_last_name, 
                c_preferred_cust_flag 
                customer_preferred_cust_flag, 
                c_birth_country                           customer_birth_country 
                , 
                c_login 
                customer_login, 
                c_email_address                           customer_email_address 
                , 
                d_year                                    dyear 
                , 
                Sum(( ( ( cs_ext_list_price 
                          - cs_ext_wholesale_cost 
                          - cs_ext_discount_amt 
                        ) + 
                              cs_ext_sales_price ) / 2 )) year_total, 
                'c'                                       sale_type 
         FROM   customer, 
                catalog_sales, 
                date_dim 
         WHERE  c_customer_sk = cs_bill_customer_sk 
                AND cs_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year 
         UNION ALL 
         SELECT c_customer_id                             customer_id, 
                c_first_name                              customer_first_name, 
                c_last_name                               customer_last_name, 
                c_preferred_cust_flag 
                customer_preferred_cust_flag, 
                c_birth_country                           customer_birth_country 
                , 
                c_login 
                customer_login, 
                c_email_address                           customer_email_address 
                , 
                d_year                                    dyear 
                , 
                Sum(( ( ( ws_ext_list_price 
                          - ws_ext_wholesale_cost 
                          - ws_ext_discount_amt 
                        ) + 
                              ws_ext_sales_price ) / 2 )) year_total, 
                'w'                                       sale_type 
         FROM   customer, 
                web_sales, 
                date_dim 
         WHERE  c_customer_sk = ws_bill_customer_sk 
                AND ws_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year)"""

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
    c_email_address;"""  # agg scalar func is not present
# ========================================================================================================


Q5_CTE_TEXT = "What is the combined financial impact of sales and returns for each website, " \
              "including sales revenue, profit, return amounts, and net losses, over a given period?"

Q5_CTE_QH = """SELECT ws_web_site_sk          AS wsr_web_site_sk, 
                                ws_sold_date_sk         AS date_sk, 
                                ws_ext_sales_price      AS sales_price, 
                                ws_net_profit           AS profit, 
                                cast(0 AS decimal(7,2)) AS return_amt, 
                                cast(0 AS decimal(7,2)) AS net_loss 
                         FROM   web_sales 
                         UNION ALL 
                         SELECT          ws_web_site_sk          AS wsr_web_site_sk, 
                                         wr_returned_date_sk     AS date_sk, 
                                         cast(0 AS decimal(7,2)) AS sales_price, 
                                         cast(0 AS decimal(7,2)) AS profit, 
                                         wr_return_amt           AS return_amt, 
                                         wr_net_loss             AS net_loss 
                         FROM            web_returns 
                         LEFT OUTER JOIN web_sales 
                         ON              ( 
                                                         wr_item_sk = ws_item_sk 
                                         AND             wr_order_number = ws_order_number)"""

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
"""  # no union

# ========================================================================================================


Q11_CTE_TEXT = "Identify the total annual sales for each customer, " \
               "distinguishing between store and web sales, while also providing " \
               "customer details such as name, preferred customer status, birth country, " \
               "login, and email address."

Q11_CTE_QH = """SELECT c_customer_id                                customer_id, 
                c_first_name                                 customer_first_name 
                , 
                c_last_name 
                customer_last_name, 
                c_preferred_cust_flag 
                   customer_preferred_cust_flag 
                    , 
                c_birth_country 
                    customer_birth_country, 
                c_login                                      customer_login, 
                c_email_address 
                customer_email_address, 
                d_year                                       dyear, 
                Sum(ss_ext_list_price - ss_ext_discount_amt) year_total, 
                's'                                          sale_type 
         FROM   customer, 
                store_sales, 
                date_dim 
         WHERE  c_customer_sk = ss_customer_sk 
                AND ss_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year 
         UNION ALL 
         SELECT c_customer_id                                customer_id, 
                c_first_name                                 customer_first_name 
                , 
                c_last_name 
                customer_last_name, 
                c_preferred_cust_flag 
                customer_preferred_cust_flag 
                , 
                c_birth_country 
                customer_birth_country, 
                c_login                                      customer_login, 
                c_email_address 
                customer_email_address, 
                d_year                                       dyear, 
                Sum(ws_ext_list_price - ws_ext_discount_amt) year_total, 
                'w'                                          sale_type 
         FROM   customer, 
                web_sales, 
                date_dim 
         WHERE  c_customer_sk = ws_bill_customer_sk 
                AND ws_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year"""

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
"""  # no union
# ========================================================================================================


Q33_subquery_TEXT = "How much total sales revenue was generated by each manufacturer of 'Books' in March 1999 across " \
                    "different sales channels (store, catalog, and web) for customers located in the GMT-5 timezone?"

Q33_subquery_QH = """(SELECT i_manufact_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )) 
                AND ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 3 
                AND ss_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -5 
         GROUP  BY i_manufact_id) UNION ALL (SELECT i_manufact_id, 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )) 
                AND cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 3 
                AND cs_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -5 
         GROUP  BY i_manufact_id) UNION ALL (SELECT i_manufact_id, 
                Sum(ws_ext_sales_price) total_sales 
         FROM   web_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )) 
                AND ws_item_sk = i_item_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 3 
                AND ws_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -5 
         GROUP  BY i_manufact_id);"""

chatgpt = """SELECT
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
"""  # no union
# ========================================================================================================


Q54_subquery_TEXT = "How can we combine and analyze sales data from both catalog and web channels to understand " \
                    "customer purchasing behavior and item sales performance across different sales platforms?"

Q54_subquery_QH = """SELECT cs_sold_date_sk     sold_date_sk, 
                        cs_bill_customer_sk customer_sk, 
                        cs_item_sk          item_sk 
                 FROM   catalog_sales 
                 UNION ALL 
                 SELECT ws_sold_date_sk     sold_date_sk, 
                        ws_bill_customer_sk customer_sk, 
                        ws_item_sk          item_sk 
                 FROM   web_sales"""

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
# no union
# ========================================================================================================

Q71_subquery_TEXT = "The query retrieves sales price, sale date, item identifier, and sale time for all transactions " \
                    "across web, catalog, and store channels in November 2001."

Q71_subquery_QH = """SELECT ws_ext_sales_price AS ext_price, 
               ws_sold_date_sk    AS sold_date_sk, 
               ws_item_sk         AS sold_item_sk, 
               ws_sold_time_sk    AS time_sk 
        FROM   web_sales, 
               date_dim 
        WHERE  d_date_sk = ws_sold_date_sk 
               AND d_moy = 11 
               AND d_year = 2001 
        UNION ALL 
        SELECT cs_ext_sales_price AS ext_price, 
               cs_sold_date_sk    AS sold_date_sk, 
               cs_item_sk         AS sold_item_sk, 
               cs_sold_time_sk    AS time_sk 
        FROM   catalog_sales, 
               date_dim 
        WHERE  d_date_sk = cs_sold_date_sk 
               AND d_moy = 11 
               AND d_year = 2001 
        UNION ALL 
        SELECT ss_ext_sales_price AS ext_price, 
               ss_sold_date_sk    AS sold_date_sk, 
               ss_item_sk         AS sold_item_sk, 
               ss_sold_time_sk    AS time_sk 
        FROM   store_sales, 
               date_dim 
        WHERE  d_date_sk = ss_sold_date_sk 
               AND d_moy = 11 
               AND d_year = 2001"""

chatgpt_Q71_sql = """SELECT 
    ss.ss_sold_date_sk AS sale_date, 
    ss.ss_sold_time_sk AS sale_time,
    ss.ss_item_sk AS item_id,
    ss.ss_net_paid AS sales_price
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2001 AND d.d_moy = 11

UNION ALL

SELECT 
    cs.cs_sold_date_sk AS sale_date, 
    cs.cs_sold_time_sk AS sale_time,
    cs.cs_item_sk AS item_id,
    cs.cs_net_paid AS sales_price
FROM catalog_sales cs
JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2001 AND d.d_moy = 11

UNION ALL

SELECT 
    ws.ws_sold_date_sk AS sale_date, 
    ws.ws_sold_time_sk AS sale_time,
    ws.ws_item_sk AS item_id,
    ws.ws_net_paid AS sales_price
FROM web_sales ws
JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2001 AND d.d_moy = 11;;
"""  # correct

# ========================================================================================================


Q60_subquery_TEXT = "How much total sales revenue was generated from jewelry items sold through store, catalog, and " \
                    "web channels in August 1999, specifically from customers located in the GMT-6 time zone?"

Q60_subquery_QH = """(SELECT i_item_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND ss_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id) UNION ALL
     (SELECT i_item_id, 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND cs_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id)UNION ALL (SELECT i_item_id, 
                Sum(ws_ext_sales_price) total_sales 
         FROM   web_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND ws_item_sk = i_item_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND ws_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id); """

gpt_Q60_subquery = """SELECT
   SUM(ss_ext_sales_price) AS store_sales_revenue,
   SUM(cs_ext_sales_price) AS catalog_sales_revenue,
   SUM(ws_ext_sales_price) AS web_sales_revenue
FROM
   store_sales
JOIN
   item ON store_sales.ss_item_sk = item.i_item_sk
JOIN
   store ON store_sales.ss_store_sk = store.s_store_sk
JOIN
   customer ON store_sales.ss_customer_sk = customer.c_customer_sk
JOIN
   date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
LEFT JOIN
   catalog_sales ON catalog_sales.cs_item_sk = item.i_item_sk
   AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
   AND catalog_sales.cs_customer_sk = customer.c_customer_sk
LEFT JOIN
   web_sales ON web_sales.ws_item_sk = item.i_item_sk
   AND web_sales.ws_sold_date_sk = date_dim.d_date_sk
   AND web_sales.ws_customer_sk = customer.c_customer_sk
WHERE
   item.i_category = 'Jewelry'
   AND date_dim.d_year = 1999
   AND date_dim.d_moy = 8
   AND customer.c_current_hdemo_sk IN (
       SELECT hd_demo_sk
       FROM household_demographics
       WHERE hd_dep_count = 0
   )
   AND customer.c_current_addr_sk IN (
       SELECT ca_address_sk
       FROM customer_address
       WHERE ca_gmt_offset = -6
   );
"""
# no union

# ========================================================================================================

q56_text_full = """Compute the monthly sales amount for march, 1998, for items with 
either of `firebrick', 'rosy' or 'white', 
across all sales channels. Only consider sales of customers residing in time zone of GMT-6 hrs. Group sales by
item and sort output by sales amount."""

Q56_hqe_seed = """(Select i_item_id, Sum(ss_ext_sales_price) as total_sales 
 From customer_address, date_dim, item, store_sales 
 Where customer_address.ca_address_sk = store_sales.ss_addr_sk
 and item.i_item_sk = store_sales.ss_item_sk
 and date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and item.i_color IN ('firebrick', 'rosy', 'white')
 and customer_address.ca_gmt_offset = -6.0
 and date_dim.d_year = 1998
 and date_dim.d_moy = 3 
 Group By i_item_id 
 Order By i_item_id asc)
 UNION ALL  
 (Select i_item_id, Sum(ws_ext_sales_price) as total_sales 
 From customer_address, date_dim, item, web_sales 
 Where customer_address.ca_address_sk = web_sales.ws_bill_addr_sk
 and date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_item_sk = web_sales.ws_item_sk
 and item.i_color IN ('rosy', 'white')
 and customer_address.ca_gmt_offset = -6.0
 and date_dim.d_year = 1998
 and date_dim.d_moy = 3 
 Group By i_item_id 
 Order By i_item_id asc)
 UNION ALL  
 (Select i_item_id, Sum(cs_ext_sales_price) as total_sales 
 From catalog_sales, customer_address, date_dim, item 
 Where catalog_sales.cs_bill_addr_sk = customer_address.ca_address_sk
 and catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_item_sk = item.i_item_sk
 and item.i_color IN ('firebrick', 'rosy', 'white')
 and customer_address.ca_gmt_offset = -6.0
 and date_dim.d_year = 1998
 and date_dim.d_moy = 3 
 Group By i_item_id 
 Order By i_item_id asc);"""
# ========================================================================================================
Q75_text = """How do annual sales and returns for men's category items vary across different brands, 
classes, categories, and manufacturers?"""

Q75_hqe_seed = """(Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity as sales_cnt, cs_ext_sales_price as sales_amt 
 From catalog_sales, date_dim, item 
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_item_sk = item.i_item_sk
 and item.i_category = 'Men' 
 Group By cs_ext_sales_price, cs_quantity, d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id 
 Order By d_year desc, i_brand_id desc, i_class_id asc, i_manufact_id desc, sales_cnt desc, sales_amt asc)
 UNION ALL  
 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity as sales_cnt, ss_ext_sales_price as sales_amt 
 From date_dim, item, store_sales 
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and item.i_item_sk = store_sales.ss_item_sk
 and item.i_category = 'Men' 
 Group By d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id, ss_ext_sales_price, ss_quantity 
 Order By i_category_id desc, d_year asc, i_manufact_id asc, sales_cnt asc, i_brand_id desc)
 UNION ALL  
 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ws_quantity as sales_cnt, ws_ext_sales_price as sales_amt 
 From date_dim, item, web_sales 
 Where item.i_item_sk = web_sales.ws_item_sk
 and date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_category = 'Men' 
 Group By d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id, ws_ext_sales_price, ws_quantity 
 Order By i_class_id asc, i_brand_id desc, d_year asc, i_category_id asc, i_manufact_id asc, sales_cnt desc);
"""

# ========================================================================================================
Q76_text_full = """Computes the average quantity, list price, discount, sales price for promotional items sold through the web
channel where the promotion is not offered by mail or in an event for given gender, marital status and
educational status."""

Q76_text_chatgpt = """This query provides an aggregated sales report across store, web, and catalog channels, analyzing sales count and revenue by year, quarter, and product category. It also considers key demographic or warehouse-related attributes to understand sales trends. The report helps in comparing channel performance, seasonal variations, and product category insights."""

Q76_hqe_seed = """(Select 'store' as channel, 'ss_hdemo_sk' as col_name, d_year, d_qoy, i_category, Count(*) as sales_cnt, Sum(ss_ext_sales_price) as sales_amt 
 From date_dim, item, store_sales 
 Where item.i_item_sk = store_sales.ss_item_sk
 and date_dim.d_date_sk = store_sales.ss_sold_date_sk 
 Group By d_qoy, d_year, i_category 
 Order By channel asc, col_name asc, d_year asc, d_qoy asc, i_category asc 
 Limit 100)
 UNION ALL  
 (Select 'web' as channel, 'ws_ship_hdemo_sk' as col_name, d_year, d_qoy, i_category, Count(*) as sales_cnt, Sum(ws_ext_sales_price) as sales_amt 
 From date_dim, item, web_sales 
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_item_sk = web_sales.ws_item_sk 
 Group By d_qoy, d_year, i_category 
 Order By channel asc, col_name asc, d_year asc, d_qoy asc, i_category asc 
 Limit 100)
 UNION ALL  
 (Select 'catalog' as channel, 'cs_warehouse_sk' as col_name, d_year, d_qoy, i_category, Count(*) as sales_cnt, Sum(cs_ext_sales_price) as sales_amt 
 From catalog_sales, date_dim, item 
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_item_sk = item.i_item_sk 
 Group By d_qoy, d_year, i_category 
 Order By channel asc, col_name asc, d_year asc, d_qoy asc, i_category asc 
 Limit 100);
"""
hq_Q76_full = """SELECT channel, 
               col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               Count(*)             sales_cnt, 
               Sum(ext_sales_price) sales_amt 
FROM   (SELECT 'store'            AS channel, 
               'ss_hdemo_sk'      col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               ss_ext_sales_price ext_sales_price 
        FROM   store_sales, 
               item, 
               date_dim 
        WHERE  ss_hdemo_sk IS NULL 
               AND ss_sold_date_sk = d_date_sk 
               AND ss_item_sk = i_item_sk 
        UNION ALL 
        SELECT 'web'              AS channel, 
               'ws_ship_hdemo_sk' col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               ws_ext_sales_price ext_sales_price 
        FROM   web_sales, 
               item, 
               date_dim 
        WHERE  ws_ship_hdemo_sk IS NULL 
               AND ws_sold_date_sk = d_date_sk 
               AND ws_item_sk = i_item_sk 
        UNION ALL 
        SELECT 'catalog'          AS channel, 
               'cs_warehouse_sk'  col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               cs_ext_sales_price ext_sales_price 
        FROM   catalog_sales, 
               item, 
               date_dim 
        WHERE  cs_warehouse_sk IS NULL 
               AND cs_sold_date_sk = d_date_sk 
               AND cs_item_sk = i_item_sk) foo 
GROUP  BY channel, 
          col_name, 
          d_year, 
          d_qoy, 
          i_category 
ORDER  BY channel, 
          col_name, 
          d_year, 
          d_qoy, 
          i_category
LIMIT 100; """
# ========================================================================================================

Q77_text = """Report the total sales, returns and profit for all three sales channels for a given 30 day period. Roll up the
results by channel and a unique channel location identifier."""

Q77_hint = """It is a union query of 3 subqueries, having following set of tables in their from clauses: {'web_sales', 'web_page', 'date_dim'}, {'catalog_sales', 'catalog_returns', 'date_dim'}, {'store_sales', 'store', 'date_dim'}
"""
# ========================================================================================================
# =====
Q80_text = """Report extended sales, extended net profit and returns in the store, catalog, and web channels for a 30 day
window for items with prices larger than $50 not promoted on television, rollup results by sales channel and
channel specific sales means (store for store sales, catalog page for catalog sales and web site for web sales)"""
Q80_hqe_seed = """(Select 'catalog channel' as channel, cp_catalog_page_id as id, Sum(cs_ext_sales_price) as sales, 0.0 as returns1, Sum(cs_net_profit) as profit 
 From catalog_page, catalog_sales, date_dim, item, promotion 
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_promo_sk = promotion.p_promo_sk
 and catalog_page.cp_catalog_page_sk = catalog_sales.cs_catalog_page_sk
 and catalog_sales.cs_item_sk = item.i_item_sk
 and promotion.p_channel_tv = 'N'
 and date_dim.d_date between '2000-08-26' and '2000-09-25'
 and item.i_current_price >= 50.01 
 Group By cp_catalog_page_id 
 Order By channel asc, id asc 
 Limit 100)
 UNION ALL  
 (Select 'web channel' as channel, web_site_id as id, Sum(ws_ext_sales_price) as sales, 0.0 as returns1, Sum(ws_net_profit) as profit 
 From date_dim, item, promotion, web_sales, web_site 
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_item_sk = web_sales.ws_item_sk
 and web_sales.ws_web_site_sk = web_site.web_site_sk
 and promotion.p_promo_sk = web_sales.ws_promo_sk
 and promotion.p_channel_tv = 'N'
 and date_dim.d_date between '2000-08-26' and '2000-09-25'
 and item.i_current_price >= 50.01 
 Group By web_site_id 
 Order By channel asc, id asc 
 Limit 100)
 UNION ALL  
 (Select 'store channel' as channel, s_store_id as id, Sum(ss_ext_sales_price) as sales, 0.0 as returns1, Sum(ss_net_profit) as profit 
 From date_dim, item, promotion, store, store_sales 
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and item.i_item_sk = store_sales.ss_item_sk
 and store.s_store_sk = store_sales.ss_store_sk
 and promotion.p_promo_sk = store_sales.ss_promo_sk
 and promotion.p_channel_tv = 'N'
 and date_dim.d_date between '2000-08-26' and '2000-09-25'
 and item.i_current_price >= 50.01 
 Group By s_store_id 
 Order By channel asc, id asc 
 Limit 100);"""
# ===================================================================================================
Q14_subquery_text = """This query calculates the average sales value across store, catalog, and web channels by considering the product of quantity sold and list price for transactions between 1999 and 2001."""

# ========================================================================================================
# ========================================================================================================

Q74_subquery_TEXT = """The query calculates the total amount spent by each customer in both store and web sales channels 
for the years 1999 and 2000, categorizing sales by channel type."""
Q74_subquery_QH = """SELECT c_customer_id    customer_id, 
                c_first_name     customer_first_name, 
                c_last_name      customer_last_name, 
                d_year           AS year1, 
                Sum(ss_net_paid) year_total, 
                's'              sale_type 
         FROM   customer, 
                store_sales, 
                date_dim 
         WHERE  c_customer_sk = ss_customer_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year IN ( 1999, 1999 + 1 ) 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   d_year 
         UNION ALL 
         SELECT c_customer_id    customer_id, 
                c_first_name     customer_first_name, 
                c_last_name      customer_last_name, 
                d_year           AS year1, 
                Sum(ws_net_paid) year_total, 
                'w'              sale_type 
         FROM   customer, 
                web_sales, 
                date_dim 
         WHERE  c_customer_sk = ws_bill_customer_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year IN ( 1999, 1999 + 1 ) 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   d_year"""
# ========================================================================================================

Q56_full_text = "Compute the monthly sales amount for a specific month in a specific year, for items with three specific colors " \
                "across all sales channels. Only consider sales of customers residing in a specific time zone. Group sales by) " \
                "item and sort output by sales amount."
# ========================================================================================================

Q66_subquery_text = """The query retrieves the monthly sales revenue and net sales for warehouses in 1998, categorized by web and catalog sales. 
It filters transactions processed by specific shipping carriers ("ZOUROS" and "ZHOU") within a given time window. 
The results include warehouse details such as name, size, city, county, state, and country. Sales figures are aggregated 
per warehouse for each month of the year, enabling performance analysis across different locations and time periods."""
# ========================================================================================================


tpcds_q7 = "Compute the average quantity, list price, discount, and sales price for promotional " \
           "items sold in stores where the " \
           "promotion is not offered by mail or a special event. Restrict the results to a specific " \
           "gender, marital and " \
           "educational status."

tpcds_q1 = "Find customers who have returned items more than 20% more often than the average customer returns for " \
           "store in a given state for a given year."

tpcds_q2 = "Report the ratios of weekly web and catalog sales increases from one year to the next year " \
           "for each week. That is, compute the increase of Monday, Tuesday, ... Sunday " \
           "sales from one year to the following."

tpcds_q4 = "Find customers who spend more money via catalog than in stores. " \
           "Identify preferred customers and their " \
           "country of origin."
