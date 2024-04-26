import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from ...src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from ...test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = True
        self.conn.config.detect_or = False
        self.conn.config.detect_oj = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_simple(self):
        query = "select n_regionkey from nation where n_name <> 'GERMANY';"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    def test_for_numeric_flter(self):
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name not LIKE 'B%' and o_orderdate >= DATE '1994-01-01';"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    def test_NEP_mukul_thesis_Q1(self):
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " \
                "sum_base_price, " \
                "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, " \
                "avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order " \
                "From lineitem Where l_shipdate <= date '1998-12-01' and l_extendedprice <> 44506.02 " \
                "Group by l_returnflag, l_linestatus " \
                "Order by l_returnflag, l_linestatus;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_Q21_mukul_thesis(self):
        query = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation " \
                "Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' " \
                "and s_nationkey = n_nationkey and n_name <> 'GERMANY' Group By s_name " \
                "Order By numwait desc, s_name Limit 100;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_Q16_sql(self):
        query = ("Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part               "
                 "Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and "
                 "p_size >=  4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, "
                 "p_type, p_size;")
        eq = self.pipeline.doJob(query)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
