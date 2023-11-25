import unittest

from mysite.unmasque.src.core.factory import find_common_items
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    INTERSECT = "INTERSECT"

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)
        self.pipeline.validate_extraction = False

    def test_intersect(self):
        a = [1, 2, 3]
        b = [2, 3, 4]
        c = [3, 4, 5]
        li = [frozenset(a), frozenset(b), frozenset(c)]
        result = find_common_items(li)
        print(result)
        self.assertEqual(len(result), 1)

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
        self.assertTrue(
            "Select c_mktsegment as segment\nFrom customer, nation, orders\nWhere c_nationkey = n_nationkey and "
            "c_custkey = o_custkey" in eq)
        # self.assertTrue(self.pipeline.correct)

    def test_abhinav_thesis_q2(self): # minimization is too slow. Need to implmenent n-ary division based minimization
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

    def test_abhinav_thesis_q3(self):
        query = "select p_container,p_retailprice,ps_availqty " \
                "from part,supplierx,partsuppx where p_partkey = ps_partkey and s_suppkey = ps_suppkey and " \
                "p_brand='Brand#45' intersect select p_container,p_retailprice,ps_availqty " \
                "from part,supplierx,partsuppx where p_partkey = ps_partkey and s_suppkey=ps_suppkey and " \
                "p_brand='Brand#15';"
        # supplier ctid = '(7, 34)' or ctid = '(18, 40)'
        # partsupp '(52, 32)', '(227,45)'
        # part: ERROR
        self.conn.connectUsingParams()
        self.conn.execute_sql(["drop table if exists partsuppx;", "drop table if exists supplierx;"])
        self.conn.closeConnection()

        self.conn.connectUsingParams()
        self.conn.execute_sql(["create table partsuppx as select * from partsupp where ctid = '(52, 32)' or ctid = '(227,45)';"])
        self.conn.execute_sql(["create table supplierx as select * from supplier where ctid = '(7, 34)' or ctid = '(18, 40)';"])
        self.conn.closeConnection()

        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.conn.connectUsingParams()
        self.conn.execute_sql(["drop table if exists partsuppx;", "drop table if exists supplierx;"])
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
