 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_customer_id                       customer_id,
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
                   d_year ;
 --- extracted query:

 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-0.5*cs_ext_discount_amt + 0.5*cs_ext_list_price + 0.5*cs_ext_sales_price - 0.5*cs_ext_wholesale_cost) as year_total, 'c' as sale_type
 From catalog_sales, customer, date_dim
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_id desc, customer_first_name asc, customer_preferred_cust_flag asc, customer_last_name asc, customer_birth_country asc, customer_login desc, customer_email_address desc, year_total asc, dyear desc)
 UNION ALL
 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-0.5*ss_ext_discount_amt + 0.5*ss_ext_list_price + 0.5*ss_ext_sales_price - 0.5*ss_ext_wholesale_cost) as year_total, 's' as sale_type
 From customer, date_dim, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and customer.c_customer_sk = store_sales.ss_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_id asc, customer_preferred_cust_flag desc, customer_last_name desc, customer_login desc, customer_first_name desc, year_total asc, customer_email_address desc, dyear asc, customer_birth_country asc);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
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
                   d_year
 --- extracted query:

 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-ss_ext_discount_amt + ss_ext_list_price) as year_total, 's' as sale_type
 From customer, date_dim, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and customer.c_customer_sk = store_sales.ss_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_first_name asc, customer_last_name desc, customer_id asc, customer_preferred_cust_flag desc, customer_login asc, customer_birth_country desc, year_total asc, customer_email_address asc, sale_type asc)
 UNION ALL
 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-ws_ext_discount_amt + ws_ext_list_price) as year_total, 'w' as sale_type
 From customer, date_dim, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and customer.c_customer_sk = web_sales.ws_bill_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_first_name asc, customer_preferred_cust_flag desc, customer_login asc, customer_birth_country desc, customer_last_name asc, year_total asc, customer_id asc, customer_email_address desc, dyear asc);
 --- END OF ONE EXTRACTION EXPERIMENT


 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT c_customer_id                       customer_id,
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
                   d_year)
 --- extracted query:

 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-0.5*ss_ext_discount_amt + 0.5*ss_ext_list_price + 0.5*ss_ext_sales_price - 0.5*ss_ext_wholesale_cost) as year_total, 's' as sale_type
 From customer, date_dim, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and customer.c_customer_sk = store_sales.ss_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_login asc, customer_first_name asc, customer_preferred_cust_flag asc, customer_id asc, customer_last_name desc, customer_email_address desc, customer_birth_country asc, year_total asc, sale_type asc, dyear asc)
 UNION ALL
 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-0.5*ws_ext_discount_amt + 0.5*ws_ext_list_price + 0.5*ws_ext_sales_price - 0.5*ws_ext_wholesale_cost) as year_total, 'w' as sale_type
 From customer, date_dim, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and customer.c_customer_sk = web_sales.ws_bill_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_id desc, customer_last_name asc, customer_first_name desc, customer_preferred_cust_flag asc, customer_login desc, customer_birth_country desc, dyear asc, year_total asc, customer_email_address desc)
 UNION ALL
 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, c_preferred_cust_flag as customer_preferred_cust_flag, c_birth_country as customer_birth_country, c_login as customer_login, c_email_address as customer_email_address, d_year as dyear, Sum(-0.5*cs_ext_discount_amt + 0.5*cs_ext_list_price + 0.5*cs_ext_sales_price - 0.5*cs_ext_wholesale_cost) as year_total, 'c' as sale_type
 From catalog_sales, customer, date_dim
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
 Group By c_birth_country, c_customer_id, c_email_address, c_first_name, c_last_name, c_login, c_preferred_cust_flag, d_year
 Order By customer_login asc, customer_id asc, customer_last_name desc, customer_preferred_cust_flag desc, customer_first_name desc, customer_birth_country asc, customer_email_address desc, year_total asc, sale_type asc);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT ws_web_site_sk          AS wsr_web_site_sk,
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
                                         AND             wr_order_number = ws_order_number)
 --- extracted query:

 (Select ws_web_site_sk as wsr_web_site_sk, ws_sold_date_sk as date_sk, ws_ext_sales_price as sales_price, ws_net_profit as profit, 0.0 as return_amt, 0.0 as net_loss
 From web_sales)
 UNION ALL
 (Select NULL as wsr_web_site_sk, 2451603 as date_sk, 0.0 as sales_price, 0.0 as profit, 4.02 as return_amt, 18.15 as net_loss
 From web_returns
 Order By wsr_web_site_sk asc, date_sk asc, return_amt asc, net_loss asc);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cs_sold_date_sk     sold_date_sk,
                        cs_bill_customer_sk customer_sk,
                        cs_item_sk          item_sk
                 FROM   catalog_sales
                 UNION ALL
                 SELECT ws_sold_date_sk     sold_date_sk,
                        ws_bill_customer_sk customer_sk,
                        ws_item_sk          item_sk
                 FROM   web_sales
 --- extracted query:

 (Select ws_sold_date_sk as sold_date_sk, ws_bill_customer_sk as customer_sk, ws_item_sk as item_sk
 From web_sales)
 UNION ALL
 (Select cs_sold_date_sk as sold_date_sk, cs_bill_customer_sk as customer_sk, cs_item_sk as item_sk
 From catalog_sales);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT ws_ext_sales_price AS ext_price,
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
               AND d_year = 2001
 --- extracted query:

 (Select ws_ext_sales_price as ext_price, d_date_sk as sold_date_sk, ws_item_sk as sold_item_sk, ws_sold_time_sk as time_sk
 From date_dim, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and date_dim.d_year = 2001
 and date_dim.d_moy = 11)
 UNION ALL
 (Select ss_ext_sales_price as ext_price, d_date_sk as sold_date_sk, ss_item_sk as sold_item_sk, ss_sold_time_sk as time_sk
 From date_dim, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and date_dim.d_year = 2001
 and date_dim.d_moy = 11)
 UNION ALL
 (Select cs_ext_sales_price as ext_price, d_date_sk as sold_date_sk, cs_item_sk as sold_item_sk, cs_sold_time_sk as time_sk
 From catalog_sales, date_dim
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and date_dim.d_year = 2001
 and date_dim.d_moy = 11);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_customer_id    customer_id,
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
                   d_year
 --- extracted query:

 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, d_year as year1, Sum(ss_net_paid) as year_total, 's' as sale_type
 From customer, date_dim, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and customer.c_customer_sk = store_sales.ss_customer_sk
 and date_dim.d_year between 1999 and 2000
 Group By c_customer_id, c_first_name, c_last_name, d_year
 Order By customer_id asc, customer_first_name asc, customer_last_name asc, year1 asc)
 UNION ALL
 (Select c_customer_id as customer_id, c_first_name as customer_first_name, c_last_name as customer_last_name, d_year as year1, Sum(ws_net_paid) as year_total, 'w' as sale_type
 From customer, date_dim, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and customer.c_customer_sk = web_sales.ws_bill_customer_sk
 and date_dim.d_year between 1999 and 2000
 Group By c_customer_id, c_first_name, c_last_name, d_year
 Order By customer_id asc, customer_first_name asc, customer_last_name asc, year1 asc);
 --- END OF ONE EXTRACTION EXPERIMENT


 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT sold_date_sk,
                sales_price
         FROM   (SELECT ws_sold_date_sk    sold_date_sk,
                        ws_ext_sales_price sales_price
                 FROM   web_sales) as web_sales_date_price
         UNION ALL
         (SELECT cs_sold_date_sk    sold_date_sk,
                 cs_ext_sales_price sales_price
          FROM   catalog_sales)
 --- extracted query:

 (Select cs_sold_date_sk as sold_date_sk, cs_ext_sales_price as sales_price
 From catalog_sales)
 UNION ALL
 (Select ws_sold_date_sk as sold_date_sk, ws_ext_sales_price as sales_price
 From web_sales);
 --- END OF ONE EXTRACTION EXPERIMENT


 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT ws_web_site_sk          AS wsr_web_site_sk,
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
                                         AND             wr_order_number = ws_order_number)
 --- extracted query:
 ;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:

(SELECT i_manufact_id,
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
         GROUP  BY i_manufact_id)
         UNION ALL

     (SELECT i_manufact_id,
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
         GROUP  BY i_manufact_id) UNION ALL
     (SELECT i_manufact_id,
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
         GROUP  BY i_manufact_id);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
Could not extract the query due to errors in subquery with FROM ['store_sales', 'item', 'date_dim', 'customer_address']
Here's what I have as a half-baked answer:
FROM(q1) = { store_sales, item, date_dim, customer_address }, FROM(q2) = { date_dim, item, catalog_sales, customer_address }, FROM(q3) = { date_dim, item, web_sales, customer_address }
whatever I could form: ;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT w_warehouse_name,
               w_warehouse_sq_ft,
               w_city,
               w_county,
               w_state,
               w_country,
               'ZOUROS'
               || ','
               || 'ZHOU' AS ship_carriers,
               d_year    AS year1,
               Sum(CASE
                     WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS jan_sales,
               Sum(CASE
                     WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS feb_sales,
               Sum(CASE
                     WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS mar_sales,
               Sum(CASE
                     WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS apr_sales,
               Sum(CASE
                     WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS may_sales,
               Sum(CASE
                     WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS jun_sales,
               Sum(CASE
                     WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS jul_sales,
               Sum(CASE
                     WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS aug_sales,
               Sum(CASE
                     WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS sep_sales,
               Sum(CASE
                     WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS oct_sales,
               Sum(CASE
                     WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS nov_sales,
               Sum(CASE
                     WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity
                     ELSE 0
                   END)  AS dec_sales,
               Sum(CASE
                     WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS jan_net,
               Sum(CASE
                     WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS feb_net,
               Sum(CASE
                     WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS mar_net,
               Sum(CASE
                     WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS apr_net,
               Sum(CASE
                     WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS may_net,
               Sum(CASE
                     WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS jun_net,
               Sum(CASE
                     WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS jul_net,
               Sum(CASE
                     WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS aug_net,
               Sum(CASE
                     WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS sep_net,
               Sum(CASE
                     WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS oct_net,
               Sum(CASE
                     WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS nov_net,
               Sum(CASE
                     WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity
                     ELSE 0
                   END)  AS dec_net
        FROM   web_sales,
               warehouse,
               date_dim,
               time_dim,
               ship_mode
        WHERE  ws_warehouse_sk = w_warehouse_sk
               AND ws_sold_date_sk = d_date_sk
               AND ws_sold_time_sk = t_time_sk
               AND ws_ship_mode_sk = sm_ship_mode_sk
               AND d_year = 1998
               AND t_time BETWEEN 7249 AND 7249 + 28800
               AND sm_carrier IN ( 'ZOUROS', 'ZHOU' )
        GROUP  BY w_warehouse_name,
                  w_warehouse_sq_ft,
                  w_city,
                  w_county,
                  w_state,
                  w_country,
                  d_year
        UNION ALL
        SELECT w_warehouse_name,
               w_warehouse_sq_ft,
               w_city,
               w_county,
               w_state,
               w_country,
               'ZOUROS'
               || ','
               || 'ZHOU' AS ship_carriers,
               d_year    AS year1,
               Sum(CASE
                     WHEN d_moy = 1 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS jan_sales,
               Sum(CASE
                     WHEN d_moy = 2 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS feb_sales,
               Sum(CASE
                     WHEN d_moy = 3 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS mar_sales,
               Sum(CASE
                     WHEN d_moy = 4 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS apr_sales,
               Sum(CASE
                     WHEN d_moy = 5 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS may_sales,
               Sum(CASE
                     WHEN d_moy = 6 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS jun_sales,
               Sum(CASE
                     WHEN d_moy = 7 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS jul_sales,
               Sum(CASE
                     WHEN d_moy = 8 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS aug_sales,
               Sum(CASE
                     WHEN d_moy = 9 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS sep_sales,
               Sum(CASE
                     WHEN d_moy = 10 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS oct_sales,
               Sum(CASE
                     WHEN d_moy = 11 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS nov_sales,
               Sum(CASE
                     WHEN d_moy = 12 THEN cs_ext_sales_price * cs_quantity
                     ELSE 0
                   END)  AS dec_sales,
               Sum(CASE
                     WHEN d_moy = 1 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS jan_net,
               Sum(CASE
                     WHEN d_moy = 2 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS feb_net,
               Sum(CASE
                     WHEN d_moy = 3 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS mar_net,
               Sum(CASE
                     WHEN d_moy = 4 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS apr_net,
               Sum(CASE
                     WHEN d_moy = 5 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS may_net,
               Sum(CASE
                     WHEN d_moy = 6 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS jun_net,
               Sum(CASE
                     WHEN d_moy = 7 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS jul_net,
               Sum(CASE
                     WHEN d_moy = 8 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS aug_net,
               Sum(CASE
                     WHEN d_moy = 9 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS sep_net,
               Sum(CASE
                     WHEN d_moy = 10 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS oct_net,
               Sum(CASE
                     WHEN d_moy = 11 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS nov_net,
               Sum(CASE
                     WHEN d_moy = 12 THEN cs_net_paid * cs_quantity
                     ELSE 0
                   END)  AS dec_net
        FROM   catalog_sales,
               warehouse,
               date_dim,
               time_dim,
               ship_mode
        WHERE  cs_warehouse_sk = w_warehouse_sk
               AND cs_sold_date_sk = d_date_sk
               AND cs_sold_time_sk = t_time_sk
               AND cs_ship_mode_sk = sm_ship_mode_sk
               AND d_year = 1998
               AND t_time BETWEEN 7249 AND 7249 + 28800
               AND sm_carrier IN ( 'ZOUROS', 'ZHOU' )
        GROUP  BY w_warehouse_name,
                  w_warehouse_sq_ft,
                  w_city,
                  w_county,
                  w_state,
                  w_country,
                  d_year);
 --- extracted query:

 (Select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, 'ZOUROS,ZHOU' as ship_carriers, 1998 as year1, 0.0 as jan_sales, Sum(0) as feb_sales, 0.0 as mar_sales, 0.0 as apr_sales, 0.0 as may_sales, 0.0 as jun_sales, 0.0 as jul_sales, 0.0 as aug_sales, 0.0 as sep_sales, 0.0 as oct_sales, 0.0 as nov_sales, -0.01*cs_ext_sales_price*(5*cs_quantity + 43*d_moy - 11074) + cs_quantity*(39.68 - 0.28*d_moy) + 271.5*d_moy - 76209.49 as dec_sales, 0.0 as jan_net, Sum(0) as feb_net, 0.0 as mar_net, 0.0 as apr_net, 0.0 as may_net, 0.0 as jun_net, 0.0 as jul_net, 0.0 as aug_net, 0.0 as sep_net, 0.0 as oct_net, 0.0 as nov_net, -0.01*cs_net_paid*(18*cs_quantity + 17*d_moy - 11387) + cs_quantity*(74.89 - 0.09*d_moy) + 54.5*d_moy - 41123.24 as dec_net
 From catalog_sales, date_dim, ship_mode, time_dim, warehouse
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_ship_mode_sk = ship_mode.sm_ship_mode_sk
 and catalog_sales.cs_sold_time_sk = time_dim.t_time_sk
 and catalog_sales.cs_warehouse_sk = warehouse.w_warehouse_sk
 and ship_mode.sm_carrier IN ('ZHOU', 'ZOUROS')
 and date_dim.d_year = 1998
 and time_dim.t_time between 7249 and 36049
 Group By w_city, w_country, w_county, w_state, w_warehouse_name, w_warehouse_sq_ft
 Order By w_warehouse_name asc, w_warehouse_sq_ft asc, w_city asc, w_county asc, w_state asc, w_country asc)
 UNION ALL
 (Select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, 'ZOUROS,ZHOU' as ship_carriers, 1998 as year1, 0.0 as jan_sales, 0.0 as feb_sales, 0.0 as mar_sales, 0.0 as apr_sales, 0.0 as may_sales, 0.0 as jun_sales, 0.0 as jul_sales, 0.0 as aug_sales, 0.0 as sep_sales, 0.0 as oct_sales, 0.0 as nov_sales, -0.01*d_moy*(8*ws_ext_sales_price - ws_quantity + 648) + ws_ext_sales_price*(81.82 - 0.09*ws_quantity) - 5.47*ws_quantity + 2994.39 as dec_sales, 0.0 as jan_net, 0.0 as feb_net, 0.0 as mar_net, 0.0 as apr_net, 0.0 as may_net, 0.0 as jun_net, 0.0 as jul_net, 0.0 as aug_net, 0.0 as sep_net, 0.0 as oct_net, 0.0 as nov_net, -0.01*d_moy*(19*ws_net_paid_inc_ship + 28*ws_quantity - 11499) + ws_net_paid_inc_ship*(99.49 - 0.17*ws_quantity) + 109.99*ws_quantity - 62200.58 as dec_net
 From date_dim, ship_mode, time_dim, warehouse, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and ship_mode.sm_ship_mode_sk = web_sales.ws_ship_mode_sk
 and time_dim.t_time_sk = web_sales.ws_sold_time_sk
 and warehouse.w_warehouse_sk = web_sales.ws_warehouse_sk
 and ship_mode.sm_carrier IN ('ZHOU', 'ZOUROS')
 and date_dim.d_year = 1998
 and time_dim.t_time between 7249 and 36049
 Group By w_city, w_country, w_county, w_state, w_warehouse_name, w_warehouse_sq_ft
 Order By w_warehouse_name asc, w_warehouse_sq_ft asc, w_city asc, w_county asc, w_state asc, w_country asc);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT 'store'            AS channel,
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
               AND cs_item_sk = i_item_sk);
 --- extracted query:
 argument of type 'NoneType' is not iterableSome problem in Regular mutation pipeline. Aborting extraction!
Could not extract the query due to errors in subquery with FROM ['date_dim', 'store_sales', 'item']
Here's what I have as a half-baked answer:
FROM(q1) = { date_dim, store_sales, item }, FROM(q2) = { catalog_sales, date_dim, item }, FROM(q3) = { web_sales, date_dim, item }
whatever I could form: ;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        cs_quantity - COALESCE(cr_return_quantity, 0)        AS
                        sales_cnt,
                        cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS
                        sales_amt
                 FROM   catalog_sales
                        JOIN item
                          ON i_item_sk = cs_item_sk
                        JOIN date_dim
                          ON d_date_sk = cs_sold_date_sk
                        LEFT JOIN catalog_returns
                               ON ( cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk )
                 WHERE  i_category = 'Men'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ss_quantity - COALESCE(sr_return_quantity, 0)     AS
                        sales_cnt,
                        ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS
                        sales_amt
                 FROM   store_sales
                        JOIN item
                          ON i_item_sk = ss_item_sk
                        JOIN date_dim
                          ON d_date_sk = ss_sold_date_sk
                        LEFT JOIN store_returns
                               ON ( ss_ticket_number = sr_ticket_number
                                    AND ss_item_sk = sr_item_sk )
                 WHERE  i_category = 'Men'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ws_quantity - COALESCE(wr_return_quantity, 0)     AS
                        sales_cnt,
                        ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS
                        sales_amt
                 FROM   web_sales
                        JOIN item
                          ON i_item_sk = ws_item_sk
                        JOIN date_dim
                          ON d_date_sk = ws_sold_date_sk
                        LEFT JOIN web_returns
                               ON ( ws_order_number = wr_order_number
                                    AND ws_item_sk = wr_item_sk )
                 WHERE  i_category = 'Men')
 --- extracted query:

 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ws_quantity as sales_cnt, ws_ext_sales_price as sales_amt
 From date_dim, item, web_sales
 Where item.i_item_sk = web_sales.ws_item_sk
 and date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_category = 'Men'
 Group By d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id, ws_ext_sales_price, ws_quantity
 Order By d_year asc, i_brand_id desc, i_class_id asc, i_category_id asc, i_manufact_id desc, sales_cnt desc, sales_amt asc)
 UNION ALL
 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity as sales_cnt, cs_ext_sales_price as sales_amt
 From catalog_sales, date_dim, item
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_item_sk = item.i_item_sk
 and item.i_category = 'Men'
 Group By cs_ext_sales_price, cs_quantity, d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id
 Order By i_brand_id asc, i_class_id asc, i_manufact_id desc, d_year desc, i_category_id desc)
 UNION ALL
 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity as sales_cnt, ss_ext_sales_price as sales_amt
 From date_dim, item, store_sales
 Where item.i_item_sk = store_sales.ss_item_sk
 and date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and item.i_category = 'Men'
 Group By d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id, ss_ext_sales_price, ss_quantity
 Order By d_year asc, i_brand_id asc, sales_cnt asc, i_manufact_id desc, sales_amt desc);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        cs_quantity - COALESCE(cr_return_quantity, 0)        AS
                        sales_cnt,
                        cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS
                        sales_amt
                 FROM   catalog_sales
                        JOIN item
                          ON i_item_sk = cs_item_sk
                        JOIN date_dim
                          ON d_date_sk = cs_sold_date_sk
                        LEFT JOIN catalog_returns
                               ON ( cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk )
                 WHERE  i_category = 'Men'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ss_quantity - COALESCE(sr_return_quantity, 0)     AS
                        sales_cnt,
                        ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS
                        sales_amt
                 FROM   store_sales
                        JOIN item
                          ON i_item_sk = ss_item_sk
                        JOIN date_dim
                          ON d_date_sk = ss_sold_date_sk
                        LEFT JOIN store_returns
                               ON ( ss_ticket_number = sr_ticket_number
                                    AND ss_item_sk = sr_item_sk )
                 WHERE  i_category = 'Men'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ws_quantity - COALESCE(wr_return_quantity, 0)     AS
                        sales_cnt,
                        ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS
                        sales_amt
                 FROM   web_sales
                        JOIN item
                          ON i_item_sk = ws_item_sk
                        JOIN date_dim
                          ON d_date_sk = ws_sold_date_sk
                        LEFT JOIN web_returns
                               ON ( ws_order_number = wr_order_number
                                    AND ws_item_sk = wr_item_sk )
                 WHERE  i_category = 'Men')
 --- extracted query:

 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity as sales_cnt, ss_ext_sales_price as sales_amt
 From date_dim, item, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and item.i_item_sk = store_sales.ss_item_sk
 and item.i_category = 'Men'
 Group By d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id, ss_ext_sales_price, ss_quantity
 Order By d_year asc, i_category_id desc, i_brand_id asc, sales_cnt desc, i_class_id desc)
 UNION ALL
 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ws_quantity as sales_cnt, ws_ext_sales_price as sales_amt
 From date_dim, item, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_item_sk = web_sales.ws_item_sk
 and item.i_category = 'Men'
 Group By d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id, ws_ext_sales_price, ws_quantity
 Order By d_year desc, i_class_id asc, i_category_id asc, i_manufact_id desc, i_brand_id desc)
 UNION ALL
 (Select d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity as sales_cnt, cs_ext_sales_price as sales_amt
 From catalog_sales, date_dim, item
 Where catalog_sales.cs_item_sk = item.i_item_sk
 and catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and item.i_category = 'Men'
 Group By cs_ext_sales_price, cs_quantity, d_year, i_brand_id, i_category_id, i_class_id, i_manufact_id
 Order By d_year asc, i_category_id desc, sales_cnt asc, i_brand_id asc, i_manufact_id desc, i_class_id desc);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT i_item_id,
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
         GROUP  BY i_item_id);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
Could not extract the query due to errors in subquery with FROM ['store_sales', 'customer_address', 'item', 'date_dim']
Here's what I have as a half-baked answer:
FROM(q1) = { store_sales, customer_address, item, date_dim }, FROM(q2) = { item, customer_address, web_sales, date_dim }, FROM(q3) = { catalog_sales, item, date_dim, customer_address }
whatever I could form: ;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT i_item_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     cs
     AS (SELECT i_item_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     ws
     AS (SELECT i_item_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id);
 --- extracted query:
 (Select i_item_id, Sum(cs_ext_sales_price) as total_sales
 From catalog_sales, customer_address, date_dim, item
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and catalog_sales.cs_bill_addr_sk = customer_address.ca_address_sk
 and catalog_sales.cs_item_sk = item.i_item_sk
 and item.i_color IN ('firebrick', 'rosy', 'white')
 and customer_address.ca_gmt_offset = -6.0
 and date_dim.d_year = 1998
 and date_dim.d_moy = 3
 Group By i_item_id
 Order By i_item_id asc)
 UNION ALL
 (Select i_item_id, Sum(ws_ext_sales_price) as total_sales
 From customer_address, date_dim, item, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and item.i_item_sk = web_sales.ws_item_sk
 and customer_address.ca_address_sk = web_sales.ws_bill_addr_sk
 and item.i_color IN ('firebrick','rosy', 'white')
 and date_dim.d_year = 1998
 and date_dim.d_moy = 3
 and customer_address.ca_gmt_offset = -6.0
 Group By i_item_id
 Order By i_item_id asc)
 UNION ALL
 (Select i_item_id, Sum(ss_ext_sales_price) as total_sales
 From customer_address, date_dim, item, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and item.i_item_sk = store_sales.ss_item_sk
 and customer_address.ca_address_sk = store_sales.ss_addr_sk
 and item.i_color IN ('firebrick', 'rosy', 'white')
 and customer_address.ca_gmt_offset = -6.0
 and date_dim.d_year = 1998
 and date_dim.d_moy = 3
 Group By i_item_id
 Order By i_item_id asc);
 --- END OF ONE EXTRACTION EXPERIMENT


 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (SELECT ss_quantity   quantity,
                        ss_list_price list_price
                 FROM   store_sales,
                        date_dim
                 WHERE  ss_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2
                 UNION ALL
                 SELECT cs_quantity   quantity,
                        cs_list_price list_price
                 FROM   catalog_sales,
                        date_dim
                 WHERE  cs_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2
                 UNION ALL
                 SELECT ws_quantity   quantity,
                        ws_list_price list_price
                 FROM   web_sales,
                        date_dim
                 WHERE  ws_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2)
 --- extracted query:

 (Select ws_quantity as quantity, ws_list_price as list_price
 From date_dim, web_sales
 Where date_dim.d_date_sk = web_sales.ws_sold_date_sk
 and date_dim.d_year between 1999 and 2001)
 UNION ALL
 (Select cs_quantity as quantity, cs_list_price as list_price
 From catalog_sales, date_dim
 Where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
 and date_dim.d_year between 1999 and 2001)
 UNION ALL
 (Select ss_quantity as quantity, ss_list_price as list_price
 From date_dim, store_sales
 Where date_dim.d_date_sk = store_sales.ss_sold_date_sk
 and date_dim.d_year between 1999 and 2001);
 --- END OF ONE EXTRACTION EXPERIMENT
