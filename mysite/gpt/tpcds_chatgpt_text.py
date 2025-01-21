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
""" # Correct

#========================================================================================================


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
    c_email_address;""" # agg scalar func is not present
#========================================================================================================


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
"""# no union

#========================================================================================================


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
""" #no union
#========================================================================================================


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
""" # no union
#========================================================================================================


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
#========================================================================================================

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
""" # correct

#========================================================================================================


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

#========================================================================================================

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