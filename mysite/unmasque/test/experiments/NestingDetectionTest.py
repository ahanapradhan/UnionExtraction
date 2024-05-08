import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.conn.config.detect_oj = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def check_assert(self, truth):
        print(truth)
        if truth is None:
            self.assertTrue(False)
        self.assertTrue(truth)

    def test_nested_sum(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select sum(o_totalprice) as total_sum " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue('SUM(o_totalprice)' in eq)

    def test_nested_sum_from_clause(self):
        query = "select c_name, c_acctbal from customer,  " \
                "(select o_custkey, sum(o_totalprice) as total_sum " \
                "from orders where o_orderstatus = 'O' group by o_custkey) as avgTable " \
                "where avgTable.o_custkey = c_custkey and " \
                "avgTable.total_sum < 12000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue('SUM(o_totalprice)' in eq)
        self.assertTrue(self.pipeline.correct)

    def test_another_nested_from_clause(self):
        query = "select o_clerk, o_totalprice from orders, (select l_orderkey, sum(l_extendedprice) as total_sum from " \
                "lineitem where l_linenumber = 4 group by l_orderkey) as avgTable where avgTable.l_orderkey = " \
                "o_orderkey and avgTable.total_sum < 12000  and o_orderpriority = '1-URGENT' order by o_totalprice " \
                "limit 10;"

        eq = self.do_test(query)
        self.assertTrue(self.pipeline.correct)

    def test_nested_avg(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select avg(o_totalprice) as custom_avg " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue('AVG(o_totalprice)' in eq)

    def test_nested_min(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select min(o_totalprice) as total_sum " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue('MIN(o_totalprice)' in eq)

    def do_test(self, query):
        eq = self.pipeline.doJob(query)
        self.pipeline.time_profile.print()
        self.check_assert(eq)
        return eq

    def test_nested_max(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select max(o_totalprice) as custom_avg " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue('MAX(o_totalprice)' in eq)

    def test_nested_sum_2_outer_tables(self):
        query = "select c_name, c_acctbal, n_name from customer, nation " \
                "where c_nationkey = n_nationkey and (select sum(o_totalprice) as total_sum " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000 and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue('SUM(o_totalprice)' in eq)
        self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()