import unittest

import pytest

from mysite.unmasque.src.core.abstract.spj_QueryStringGenerator import SPJQueryStringGenerator
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class TestProjectData:
    projection_names = []
    projected_attribs = []


class MyTestCase(BaseTestCase):
    INTERSECT = "INTERSECT"

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)
        self.pipeline.validate_extraction = True

    def test_brazil_argentina_basic(self):
        query = "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'BRAZIL' INTERSECT " \
                "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'ARGENTINA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        subq_check = eq.count(
            "(Select c_mktsegment as segment\nFrom customer, nation\nWhere c_nationkey = n_nationkey and n_name  = "
            "'BRAZIL')")
        self.assertEqual(subq_check, 1)
        subq_check = eq.count(
            "(Select c_mktsegment as segment\nFrom customer, nation\nWhere c_nationkey = n_nationkey and n_name  = "
            "'ARGENTINA')")
        self.assertEqual(subq_check, 1)
        self.assertTrue(self.pipeline.correct)

    def test_brazil_argentina_2(self):
        query = "select c_mktsegment as segment from customer,nation " \
                "where c_acctbal < 3000 and c_nationkey = n_nationkey and n_name = 'BRAZIL' " \
                "intersect " \
                "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name = 'ARGENTINA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue("Select c_mktsegment as segment\nFrom customer, nation\nWhere c_nationkey = n_nationkey" in eq)
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_custom_date(self):
        query = "select l_shipdate as checked_date from lineitem, orders " \
                "where l_orderkey = o_orderkey  " \
                "and o_orderstatus = 'F' " \
                "INTERSECT " \
                "select o_orderdate as checked_date from orders, lineitem, customer " \
                "where l_orderkey = o_orderkey " \
                "and o_custkey = c_custkey " \
                "and o_orderstatus = 'O' " \
                "and l_shipmode = 'RAIL' and c_acctbal < 1000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue(self.pipeline.correct)

    def test_custom_date2(self):
        query = "select l_shipdate as checked_date, l_returnflag, l_shipinstruct from lineitem, orders " \
                "where l_orderkey = o_orderkey  " \
                "and o_orderstatus = 'F' " \
                "INTERSECT " \
                "select o_orderdate as checked_date, l_returnflag, l_shipinstruct from orders, lineitem, customer " \
                "where l_orderkey = o_orderkey " \
                "and o_custkey = c_custkey " \
                "and o_orderstatus = 'O' " \
                "and l_shipmode = 'RAIL' and c_acctbal < 1000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_custom_date3(self):
        query = "select l_shipdate as checked_date, l_shipinstruct from lineitem, partsupp  " \
                "where l_partkey = ps_partkey and l_suppkey = ps_suppkey " \
                "and ps_supplycost < 1000 and l_returnflag = 'A' " \
                "INTERSECT select l_commitdate as checked_date, l_shipinstruct " \
                "from lineitem where l_returnflag = 'N';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_custom_date4(self):
        query = "select l_shipdate as checked_date, l_shipinstruct from lineitem, partsupp  " \
                "where l_partkey = ps_partkey and l_suppkey = ps_suppkey " \
                "and ps_supplycost < 1000 and l_returnflag = 'A' " \
                "INTERSECT " \
                "select l_commitdate as checked_date, l_shipinstruct from lineitem, part, partsupp " \
                "where l_returnflag = 'N' and l_partkey = p_partkey and l_suppkey = ps_suppkey " \
                "and p_container = 'LG CASE' and ps_availqty > 5000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_abhinav_thesis_q2(self):  # minimization is too slow. Need to implmenent n-ary division based minimization
        query = "select o_orderstatus, o_totalprice " \
                "from customer,orders where c_custkey = o_custkey and o_orderdate < date '1995-03-10' " \
                "intersect " \
                "select o_orderstatus, o_totalprice from lineitem, orders " \
                "where o_orderkey = l_orderkey and o_orderdate > date '1995-03-10' and l_shipmode = 'AIR';"
        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue("Select o_orderstatus, o_totalprice\nFrom customer, orders\nWhere c_custkey = o_custkey")
        self.assertTrue("Select o_orderstatus, o_totalprice\nFrom lineitem, orders\nWhere l_orderkey = o_orderkey")
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_abhinav_thesis_q3(self):
        query = "select p_container,p_retailprice,ps_availqty " \
                "from part,supplier,partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey and " \
                "p_brand='Brand#45' intersect select p_container,p_retailprice,ps_availqty " \
                "from part,supplier,partsupp where p_partkey = ps_partkey and s_suppkey=ps_suppkey and " \
                "p_brand='Brand#15';"

        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue(self.pipeline.correct)

    def test_q_sample(self):
        q_gen = SPJQueryStringGenerator(self.conn)
        pj = TestProjectData()
        pj.projection_names.append('o_orderstatus')
        pj.projection_names.append('o_totalprice')

        pj.projected_attribs.append('o_orderstatus')
        pj.projected_attribs.append('o_totalprice')
        pjs = [pj, pj, pj]
        q_gen.refine_Query1(pjs)
        self.assertEqual(q_gen.select_op, "o_orderstatus, o_totalprice")


if __name__ == '__main__':
    unittest.main()
