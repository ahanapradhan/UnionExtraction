

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

