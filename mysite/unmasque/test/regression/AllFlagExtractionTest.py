import random
import unittest
from datetime import date, timedelta

import pytest

from mysite.gpt.tpcds_benchmark_queries import Q4_CTE, Q2_subquery, Q5_CTE, Q71_subquery, Q11_CTE, Q74_subquery, \
    Q54_subquery
from ...src.core.factory.PipeLineFactory import PipeLineFactory
from ..util import queries
from ..util.BaseTestCase import BaseTestCase


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
        query = """(SELECT 'store'            AS channel, 
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
               AND cs_item_sk = i_item_sk);"""
        self.do_test(query)

    def test_Q75(self):
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

    def test_Q14_subquery(self):
        query = """(SELECT ss_quantity   quantity, 
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
                        AND d_year BETWEEN 1999 AND 1999 + 2)"""
        self.do_test(query)

    def test_Q56(self):
        query = """(SELECT i_item_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                             ) 
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
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                             ) 
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
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                             ) 
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
        self.conn.config.use_cs2 = False
        self.do_test(query)


if __name__ == '__main__':
    unittest.main()
