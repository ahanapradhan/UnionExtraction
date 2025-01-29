import random
import unittest
from datetime import date, timedelta

from mysite.gpt.tpcds_benchmark_queries import Q4_CTE, Q2_subquery, Q5_CTE, Q71_subquery, Q11_CTE, Q74_subquery, \
    Q54_subquery
from ..util.BaseTestCase import BaseTestCase
from ...src.core.factory.PipeLineFactory import PipeLineFactory


def generate_random_dates():
    start_date = date(1992, 3, 3)
    end_date = date(1998, 12, 5)

    # Generate two random dates
    random_date1 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    random_date2 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    # Return dates in a tuple with the lesser value first
    dates = min(random_date1, random_date2), max(random_date1, random_date2)
    return f"\'{str(dates[0])}\'", f"\'{str(dates[1])}\'"


class ExtractionTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn.config.detect_union = True
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_or = False
        self.conn.config.use_cs2 = False
        self.pipeline = None

    def setUp(self):
        super().setUp()
        del self.pipeline

    def do_test(self, query):
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        u_Q = self.pipeline.doJob(query)
        print(u_Q)
        record_file = open("extraction_result.sql", "a")
        record_file.write("\n --- START OF ONE EXTRACTION EXPERIMENT\n")
        record_file.write(" --- input query:\n ")
        record_file.write(query)
        record_file.write("\n")
        record_file.write(" --- extracted query:\n ")
        if u_Q is None:
            u_Q = '--- Extraction Failed! Nothing to show! '
        record_file.write(u_Q)
        record_file.write("\n --- END OF ONE EXTRACTION EXPERIMENT\n")
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        del factory

    def test_Q10(self):
        query = """SELECT cd_gender, 
               cd_marital_status, 
               cd_education_status, 
               Count(*) cnt1, 
               cd_purchase_estimate, 
               Count(*) cnt2, 
               cd_credit_rating, 
               Count(*) cnt3, 
               cd_dep_count, 
               Count(*) cnt4, 
               cd_dep_employed_count, 
               Count(*) cnt5, 
               cd_dep_college_count, 
               Count(*) cnt6 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_county IN ( 'Lycoming County', 'Sheridan County', 
                          'Kandiyohi County', 
                          'Pike County', 
                                           'Greene County' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 1999
                          AND d_moy BETWEEN 4 AND 4 + 3) 
       AND ( EXISTS (SELECT * 
                     FROM   web_sales, 
                            date_dim1 
                     WHERE  c.c_customer_sk = ws_bill_customer_sk 
                            AND ws_sold_date_sk = d1_date_sk 
                          AND d1_year = 1999
                            AND d1_moy BETWEEN 4 AND 4 + 3) 
              OR EXISTS (SELECT * 
                         FROM   catalog_sales, 
                                date_dim1 
                         WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                AND cs_sold_date_sk = d1_date_sk 
                              AND d1_year = 1999
                                AND d1_moy BETWEEN 4 AND 4 + 3) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count
LIMIT 100; """
        self.conn.config.detect_union = False
        self.conn.config.detect_or = True
        self.do_test(query)

    def test_Q4(self):
        query = Q4_CTE
        self.do_test(query)

    def test_Q2(self):
        query = Q2_subquery
        self.do_test(query)

    def test_Q71(self):
        query = Q71_subquery
        self.do_test(query)

    def test_Q54(self):
        query = Q54_subquery
        self.do_test(query)

    def test_Q5(self):
        query = Q5_CTE
        self.conn.config.detect_oj = True
        self.do_test(query)

    def test_Q11(self):
        query = Q11_CTE
        self.do_test(query)

    def test_Q74(self):
        query = Q74_subquery
        self.conn.config.detect_or = True
        self.do_test(query)

    def test_Q33(self):
        query = """ 

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
         GROUP  BY i_manufact_id);"""
        self.do_test(query)

    def test_Q66(self):
        self.conn.config.detect_or = True
        query = """(SELECT w_warehouse_name, 
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
                  d_year);"""
        self.do_test(query)

    def test_Q76(self):
        query = """SELECT channel, 
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
        WHERE ss_sold_date_sk = d_date_sk 
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
        WHERE  ws_sold_date_sk = d_date_sk 
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
        WHERE  cs_sold_date_sk = d_date_sk 
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
        self.do_test(query)

    def test_Q77(self):
        self.conn.config.detect_union = True
        self.conn.config.detect_oj = True
        query = """
WITH ss AS 
( 
         SELECT   s_store_sk, 
                  Sum(ss_ext_sales_price) AS sales, 
                  Sum(ss_net_profit)      AS profit 
         FROM     store_sales, 
                  date_dim, 
                  store 
         WHERE    ss_sold_date_sk = d_date_sk 
         AND      d_date BETWEEN Cast('2001-08-16' AS DATE) AND      ( 
                           Cast('2001-08-16' AS DATE) + INTERVAL '30' day) 
         AND      ss_store_sk = s_store_sk 
         GROUP BY s_store_sk) , sr AS 
( 
         SELECT   s_store_sk, 
                  sum(sr_return_amt) AS returns1, 
                  sum(sr_net_loss)   AS profit_loss 
         FROM     store_returns, 
                  date_dim, 
                  store 
         WHERE    sr_returned_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         AND      sr_store_sk = s_store_sk 
         GROUP BY s_store_sk), cs AS 
( 
         SELECT   cs_call_center_sk, 
                  sum(cs_ext_sales_price) AS sales, 
                  sum(cs_net_profit)      AS profit 
         FROM     catalog_sales, 
                  date_dim 
         WHERE    cs_sold_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         GROUP BY cs_call_center_sk ), cr AS 
( 
         SELECT   cr_call_center_sk, 
                  sum(cr_return_amount) AS returns1, 
                  sum(cr_net_loss)      AS profit_loss 
         FROM     catalog_returns, 
                  date_dim 
         WHERE    cr_returned_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         GROUP BY cr_call_center_sk ), ws AS 
( 
         SELECT   wp_web_page_sk, 
                  sum(ws_ext_sales_price) AS sales, 
                  sum(ws_net_profit)      AS profit 
         FROM     web_sales, 
                  date_dim, 
                  web_page 
         WHERE    ws_sold_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         AND      ws_web_page_sk = wp_web_page_sk 
         GROUP BY wp_web_page_sk), wr AS 
( 
         SELECT   wp_web_page_sk, 
                  sum(wr_return_amt) AS returns1, 
                  sum(wr_net_loss)   AS profit_loss 
         FROM     web_returns, 
                  date_dim, 
                  web_page 
         WHERE    wr_returned_date_sk = d_date_sk 
         AND      d_date BETWEEN cast('2001-08-16' AS date) AND      ( 
                           cast('2001-08-16' AS date) + INTERVAL '30' day) 
         AND      wr_web_page_sk = wp_web_page_sk 
         GROUP BY wp_web_page_sk) 
SELECT
         channel , 
         id , 
         sum(sales)   AS sales , 
         sum(returns1) AS returns1 , 
         sum(profit)  AS profit 
FROM     ( 
                   SELECT    'store channel' AS channel , 
                             ss.s_store_sk   AS id , 
                             sales , 
                             COALESCE(returns1, 0)               AS returns1 , 
                             (profit - COALESCE(profit_loss,0)) AS profit 
                   FROM      ss 
                   LEFT JOIN sr 
                   ON        ss.s_store_sk = sr.s_store_sk 
                   UNION ALL 
                   SELECT 'catalog channel' AS channel , 
                          cs_call_center_sk AS id , 
                          sales , 
                          returns1 , 
                          (profit - profit_loss) AS profit 
                   FROM   cs , 
                          cr 
                   UNION ALL 
                   SELECT    'web channel'     AS channel , 
                             ws.wp_web_page_sk AS id , 
                             sales , 
                             COALESCE(returns1, 0)                  returns1 , 
                             (profit - COALESCE(profit_loss,0)) AS profit 
                   FROM      ws 
                   LEFT JOIN wr 
                   ON        ws.wp_web_page_sk = wr.wp_web_page_sk ) x 
GROUP BY channel, id 
ORDER BY channel , 
         id 
LIMIT 100; """
        self.do_test(query)

    def test_Q75(self):
        self.conn.config.detect_oj = True
        query = """(SELECT d_year, 
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
                 WHERE  i_category = 'Men')"""
        self.conn.config.detect_oj = True
        self.do_test(query)

    def test_Q80(self):
        query = """SELECT
         channel , 
         id , 
         sum(sales)   AS sales , 
         sum(returns1) AS returns1 , 
         sum(profit)  AS profit 
FROM     ( 
                SELECT 'store channel' AS channel , 
                       'store' 
                              || store_id AS id , 
                       sales , 
                       returns1 , 
                       profit 
                FROM   ( 
                SELECT          s_store_id                                    AS store_id, 
                                Sum(ss_ext_sales_price)                       AS sales, 
                                Sum(COALESCE(sr_return_amt, 0))               AS returns1, 
                                Sum(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit 
                FROM            store_sales 
                LEFT OUTER JOIN store_returns 
                ON              ( 
                                                ss_item_sk = sr_item_sk 
                                AND             ss_ticket_number = sr_ticket_number), 
                                date_dim, 
                                store, 
                                item, 
                                promotion 
                WHERE           ss_sold_date_sk = d_date_sk 
                AND             d_date BETWEEN Cast('2000-08-26' AS DATE) AND             ( 
                                                Cast('2000-08-26' AS DATE) + INTERVAL '30' day) 
                AND             ss_store_sk = s_store_sk 
                AND             ss_item_sk = i_item_sk 
                AND             i_current_price > 50 
                AND             ss_promo_sk = p_promo_sk 
                AND             p_channel_tv = 'N' 
                GROUP BY        s_store_id) as ssr
                UNION ALL 
                SELECT 'catalog channel' AS channel , 
                       'catalog_page' 
                              || catalog_page_id AS id , 
                       sales , 
                       returns1 , 
                       profit 
                FROM   ( 
                SELECT          cp_catalog_page_id                            AS catalog_page_id, 
                                sum(cs_ext_sales_price)                       AS sales, 
                                sum(COALESCE(cr_return_amount, 0))            AS returns1, 
                                sum(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit 
                FROM            catalog_sales 
                LEFT OUTER JOIN catalog_returns 
                ON              ( 
                                                cs_item_sk = cr_item_sk 
                                AND             cs_order_number = cr_order_number), 
                                date_dim, 
                                catalog_page, 
                                item, 
                                promotion 
                WHERE           cs_sold_date_sk = d_date_sk 
                AND             d_date BETWEEN cast('2000-08-26' AS date) AND             ( 
                                                cast('2000-08-26' AS date) + INTERVAL '30' day) 
                AND             cs_catalog_page_sk = cp_catalog_page_sk 
                AND             cs_item_sk = i_item_sk 
                AND             i_current_price > 50 
                AND             cs_promo_sk = p_promo_sk 
                AND             p_channel_tv = 'N' 
                GROUP BY        cp_catalog_page_id) csr
                UNION ALL 
                SELECT 'web channel' AS channel , 
                       'web_site' 
                              || web_site_id AS id , 
                       sales , 
                       returns1 , 
                       profit 
                FROM   ( 
                SELECT          web_site_id, 
                                sum(ws_ext_sales_price)                       AS sales, 
                                sum(COALESCE(wr_return_amt, 0))               AS returns1, 
                                sum(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit 
                FROM            web_sales 
                LEFT OUTER JOIN web_returns 
                ON              ( 
                                                ws_item_sk = wr_item_sk 
                                AND             ws_order_number = wr_order_number), 
                                date_dim, 
                                web_site, 
                                item, 
                                promotion 
                WHERE           ws_sold_date_sk = d_date_sk 
                AND             d_date BETWEEN cast('2000-08-26' AS date) AND             ( 
                                                cast('2000-08-26' AS date) + INTERVAL '30' day) 
                AND             ws_web_site_sk = web_site_sk 
                AND             ws_item_sk = i_item_sk 
                AND             i_current_price > 50 
                AND             ws_promo_sk = p_promo_sk 
                AND             p_channel_tv = 'N' 
                GROUP BY        web_site_id) wsr) x 
GROUP BY channel, id 
ORDER BY channel , 
         id 
LIMIT 100; """
        self.conn.config.detect_oj = True
        self.conn.config.detect_union = True
        self.do_test(query)

    def test_Q60(self):
        query = """(SELECT i_item_id, 
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
        self.conn.config.detect_or = True
        self.do_test(query)

    def test_Q23(self):
        query = """
WITH frequent_ss_items 
     AS (SELECT Substr(i_item_desc, 1, 30) itemdesc, 
                i_item_sk                  item_sk, 
                d_date                     solddate, 
                Count(*)                   cnt 
         FROM   store_sales, 
                date_dim, 
                item 
         WHERE  ss_sold_date_sk = d_date_sk 
                AND ss_item_sk = i_item_sk 
                AND d_year IN ( 2000 , 2000  + 1, 2000  + 2, 2000  + 3 ) 
         GROUP  BY Substr(i_item_desc, 1, 30), 
                   i_item_sk, 
                   d_date 
         HAVING Count(*) > 1),
	max_store_sales 
     AS (SELECT Max(csales) tpcds_cmax 
         FROM   (SELECT c_customer_sk, 
                        Sum(ss_quantity * ss_sales_price) csales 
                 FROM   store_sales, 
                        customer, 
                        date_dim 
                 WHERE  ss_customer_sk = c_customer_sk 
                        AND ss_sold_date_sk = d_date_sk 
                        AND d_year IN ( 2000 , 2000  + 1, 2000  + 2, 2000  + 3 ) 
                 GROUP  BY c_customer_sk) foo1),
    best_ss_customer 
     AS (SELECT c_customer_sk, 
                Sum(ss_quantity * ss_sales_price) ssales 
         FROM   store_sales, 
                customer 
         WHERE  ss_customer_sk = c_customer_sk 
         GROUP  BY c_customer_sk 
         HAVING Sum(ss_quantity * ss_sales_price) > 
                ( 95 / 100.0 ) * (SELECT * 
                                  FROM   max_store_sales))
SELECT c_last_name, 
               c_first_name, 
               sales 
FROM   (SELECT c_last_name, 
               c_first_name, 
               Sum(cs_quantity * cs_list_price) sales 
        FROM   catalog_sales, 
               customer, 
               date_dim 
        WHERE  d_year = 2000  
               AND d_moy = 6 
               AND cs_sold_date_sk = d_date_sk 
               AND cs_item_sk IN (SELECT item_sk 
                                  FROM   frequent_ss_items) 
               AND cs_bill_customer_sk IN (SELECT c_customer_sk 
                                           FROM   best_ss_customer) 
               AND cs_bill_customer_sk = c_customer_sk 
        GROUP  BY c_last_name, 
                  c_first_name 
        UNION ALL 
        SELECT c_last_name, 
               c_first_name, 
               Sum(ws_quantity * ws_list_price) sales 
        FROM   web_sales, 
               customer, 
               date_dim 
        WHERE  d_year = 2000 
               AND d_moy = 6 
               AND ws_sold_date_sk = d_date_sk 
               AND ws_item_sk IN (SELECT item_sk 
                                  FROM   frequent_ss_items) 
               AND ws_bill_customer_sk IN (SELECT c_customer_sk 
                                           FROM   best_ss_customer) 
               AND ws_bill_customer_sk = c_customer_sk 
        GROUP  BY c_last_name, 
                  c_first_name) foo
ORDER  BY c_last_name, 
          c_first_name, 
          sales
LIMIT 100; """
        self.conn.config.detect_union = True
        #self.conn.config.detect_oj = True
        self.do_test(query)

    def test_Q14_subquery(self):
        query = """SELECT Avg(quantity * list_price) average_sales 
         FROM   (SELECT ss_quantity   quantity, 
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
                        AND d_year BETWEEN 1999 AND 1999 + 2) x"""
        self.do_test(query)

    def test_Q49(self):
        self.conn.config.detect_union = True
        self.conn.config.detect_or = True
        query = """-- start query 49 in stream 0 using template query49.tpl 
SELECT 'web' AS channel, 
               web.item, 
               web.return_ratio, 
               web.return_rank, 
               web.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               Rank() 
                 OVER ( 
                   ORDER BY return_ratio)   AS return_rank, 
               Rank() 
                 OVER ( 
                   ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT ws.ws_item_sk                                       AS 
                       item, 
                       ( Cast(Sum(COALESCE(wr.wr_return_quantity, 0)) AS DEC(15, 
                              4)) / 
                         Cast( 
                         Sum(COALESCE(ws.ws_quantity, 0)) AS DEC(15, 4)) ) AS 
                       return_ratio, 
                       ( Cast(Sum(COALESCE(wr.wr_return_amt, 0)) AS DEC(15, 4)) 
                         / Cast( 
                         Sum( 
                         COALESCE(ws.ws_net_paid, 0)) AS DEC(15, 
                         4)) )                                             AS 
                       currency_ratio 
                FROM   web_sales ws 
                       LEFT OUTER JOIN web_returns wr 
                                    ON ( ws.ws_order_number = wr.wr_order_number 
                                         AND ws.ws_item_sk = wr.wr_item_sk ), 
                       date_dim 
                WHERE  wr.wr_return_amt > 10000 
                       AND ws.ws_net_profit > 1 
                       AND ws.ws_net_paid > 0 
                       AND ws.ws_quantity > 0 
                       AND ws_sold_date_sk = d_date_sk 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY ws.ws_item_sk) in_web) web 
WHERE  ( web.return_rank <= 10 
          OR web.currency_rank <= 10 ) 
UNION 
SELECT 'catalog' AS channel, 
       catalog.item, 
       catalog.return_ratio, 
       catalog.return_rank, 
       catalog.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               Rank() 
                 OVER ( 
                   ORDER BY return_ratio)   AS return_rank, 
               Rank() 
                 OVER ( 
                   ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT cs.cs_item_sk                                       AS 
                       item, 
                       ( Cast(Sum(COALESCE(cr.cr_return_quantity, 0)) AS DEC(15, 
                              4)) / 
                         Cast( 
                         Sum(COALESCE(cs.cs_quantity, 0)) AS DEC(15, 4)) ) AS 
                       return_ratio, 
                       ( Cast(Sum(COALESCE(cr.cr_return_amount, 0)) AS DEC(15, 4 
                              )) / 
                         Cast(Sum( 
                         COALESCE(cs.cs_net_paid, 0)) AS DEC( 
                         15, 4)) )                                         AS 
                       currency_ratio 
                FROM   catalog_sales cs 
                       LEFT OUTER JOIN catalog_returns cr 
                                    ON ( cs.cs_order_number = cr.cr_order_number 
                                         AND cs.cs_item_sk = cr.cr_item_sk ), 
                       date_dim 
                WHERE  cr.cr_return_amount > 10000 
                       AND cs.cs_net_profit > 1 
                       AND cs.cs_net_paid > 0 
                       AND cs.cs_quantity > 0 
                       AND cs_sold_date_sk = d_date_sk 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY cs.cs_item_sk) in_cat) catalog 
WHERE  ( catalog.return_rank <= 10 
          OR catalog.currency_rank <= 10 ) 
UNION 
SELECT 'store' AS channel, 
       store.item, 
       store.return_ratio, 
       store.return_rank, 
       store.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               Rank() 
                 OVER ( 
                   ORDER BY return_ratio)   AS return_rank, 
               Rank() 
                 OVER ( 
                   ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT sts.ss_item_sk                                       AS 
                       item, 
                       ( Cast(Sum(COALESCE(sr.sr_return_quantity, 0)) AS DEC(15, 
                              4)) / 
                         Cast( 
                         Sum(COALESCE(sts.ss_quantity, 0)) AS DEC(15, 4)) ) AS 
                       return_ratio, 
                       ( Cast(Sum(COALESCE(sr.sr_return_amt, 0)) AS DEC(15, 4)) 
                         / Cast( 
                         Sum( 
                         COALESCE(sts.ss_net_paid, 0)) AS DEC(15, 4)) )     AS 
                       currency_ratio 
                FROM   store_sales sts 
                       LEFT OUTER JOIN store_returns sr 
                                    ON ( sts.ss_ticket_number = 
                                         sr.sr_ticket_number 
                                         AND sts.ss_item_sk = sr.sr_item_sk ), 
                       date_dim 
                WHERE  sr.sr_return_amt > 10000 
                       AND sts.ss_net_profit > 1 
                       AND sts.ss_net_paid > 0 
                       AND sts.ss_quantity > 0 
                       AND ss_sold_date_sk = d_date_sk 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY sts.ss_item_sk) in_store) store 
WHERE  ( store.return_rank <= 10 
          OR store.currency_rank <= 10 ) 
ORDER  BY 1, 
          4, 
          5
LIMIT 100; """
        self.do_test(query)

    def test_Q56(self):
        self.conn.config.detect_union = True
        query = """(SELECT i_item_id, 
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
         GROUP  BY i_item_id) UNION ALL
     (SELECT i_item_id, 
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
         GROUP  BY i_item_id) UNION ALL
     (SELECT i_item_id, 
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
         GROUP  BY i_item_id);"""
        self.conn.config.detect_or = True

        self.do_test(query)

    def test_JOB_query(self):
        query = """SELECT MIN(n_name) AS of_person, MIN(t_title) AS biography_movie
    FROM aka_name AS an,
    cast_info AS ci,
    info_type AS it,
    link_type AS lt,
    movie_link AS ml,
    name AS n,
    person_info AS pi,
    title AS t
    WHERE an_name LIKE '%a%' 
    AND n_name_pcode_cf LIKE 'U4%'
    AND it_info = 'mini biography'
    AND lt_link = 'features'
    AND pi_note = 'Volker Boehm'
    AND t_production_year BETWEEN 1980 AND 1984
    AND n_id = an_person_id
    AND n_id = pi_person_id
    AND ci_person_id = n_id
    AND t_id = ci_movie_id
    AND ml_linked_movie_id = t_id
    AND lt_id = ml_link_type_id
    AND it_id = pi_info_type_id
    AND pi_person_id = an_person_id
    AND pi_person_id = ci_person_id
    AND an_person_id = ci_person_id
    AND ci_movie_id = ml_linked_movie_id;
        """
        self.conn.config.detect_union = False
        self.conn.config.limit = 3
        self.do_test(query)

    def test_Q87(self):
        self.conn.config.detect_union = False
        query = """select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
) cool_cust
;
"""
        self.do_test(query)

    def test_Q82(self):
        query = """ Select i_item_id,i_item_desc,i_current_price
From item, inventory, date_dim, store_sales
Where i_current_price between 45 and 45 + 30 
and inv_item_sk = i_item_sk 
and d_date_sk=inv_date_sk 
and d_date between date '1999-07-09' and date '1999-09-09' and
i_manufact_id between 169 and 639 
and inv_quantity_on_hand between 100 and 500 
and ss_item_sk = i_item_sk
Group By i_item_id,i_item_desc,i_current_price
Order by i_item_id
Limit 100 ; """
        self.conn.config.detect_or = False
        self.conn.config.detect_union = False
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = False
        self.do_test(query)


if __name__ == '__main__':
    unittest.main()
