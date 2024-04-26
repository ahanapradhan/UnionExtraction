import unittest

import pytest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from ...test.util.BaseTestCase import BaseTestCase


class DisjunctionTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = True
        self.conn.config.detect_oj = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_sumang_thesis_Q2(self):
        query = "select c_mktsegment,MAX(c_acctbal) from customer where c_nationkey IN (1, 3, 9, 15, 22) group by " \
                "c_mktsegment;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_one_table_duplicate_value_columns(self):
        query = "select max(l_extendedprice) from lineitem where l_linenumber IN (1, 4);"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_sumang_thesis_Q2_1(self):
        query = "select c_mktsegment,MAX(c_acctbal) from customer where c_nationkey IN (1, 2, 5, 10) group by " \
                "c_mktsegment;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_sumang_thesis_Q3(self):
        query = "select l_shipmode,sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and (l_quantity =42 or l_quantity =50 or l_quantity=24) group by l_shipmode order by l_shipmode " \
                 "limit 100;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_sumang_thesis_Q4(self):
        query = "select AVG(l_extendedprice) as avgTOTAL from lineitem,part " \
                "where p_partkey = l_partkey and (p_brand = 'Brand#52' or p_brand = 'Brand#12') and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE');"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_sumang_thesis_Q4_prelim(self):
        query = "select AVG(l_extendedprice) as avgTOTAL from lineitem, part " \
                "where p_partkey = l_partkey and p_brand = 'Brand#52' and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE');"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_for_disjunction(self):
        query = f"select c_mktsegment as segment from customer,nation,orders, lineitem where " \
                f"c_acctbal between 9000 and 10000 and c_nationkey = " \
                f"n_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey " \
                f"and n_name IN ('BRAZIL', 'INDIA', 'ARGENTINA') " \
                f"and l_shipdate IN (DATE '1994-12-13', DATE '1998-03-15')"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    @pytest.mark.skip
    def test_sumang_thesis_Q6(self):
        query = "select n_name,SUM(s_acctbal) from supplier,partsupp,nation where ps_suppkey=s_suppkey and " \
                "s_nationkey=n_nationkey and (n_name ='ARGENTINA' or n_regionkey =3) and (s_acctbal > 2000 or " \
                "s_acctbal < 700) group by n_name ORDER BY n_name LIMIT 10;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()


if __name__ == '__main__':
    unittest.main()
