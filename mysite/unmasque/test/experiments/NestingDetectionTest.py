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
        self.assertTrue("SUM(o_totalprice)" in eq)

    def test_nested_avg(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select avg(o_totalprice) as custom_avg " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue("AVG(o_totalprice)" in eq)

    def test_nested_min(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select min(o_totalprice) as total_sum " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue("MIN(o_totalprice)" in eq)

    def do_test(self, query):
        eq = self.pipeline.extract(query)
        self.check_assert(eq)
        return eq

    def test_nested_max(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select max(o_totalprice) as custom_avg " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O') " \
                "< 120000  and c_mktsegment = 'BUILDING' limit 5;"
        eq = self.do_test(query)
        self.assertTrue("MAX(o_totalprice)" in eq)


if __name__ == '__main__':
    unittest.main()
