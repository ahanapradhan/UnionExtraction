import unittest

from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    INTERSECT = "INTERSECT"

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)
        self.pipeline.validate_extraction = False

    def test_brazil_argentina_basic(self):
        query = "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'BRAZIL' INTERSECT " \
                "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'ARGENTINA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        subq_check = eq.count("From customer, nation\nWhere c_nationkey = n_nationkey)")
        self.assertEqual(subq_check, 2)
        # self.assertTrue(self.pipeline.correct)

    def test_brazil_argentina_2(self):
        query = "select c_mktsegment as segment from customer,nation " \
                "where c_acctbal < 3000 and c_nationkey = n_nationkey and n_name = 'BRAZIL' " \
                "intersect " \
                "select c_mktsegment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name = 'ARGENTINA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        check = eq.count(self.INTERSECT)
        self.assertEqual(check, 1)
        self.assertTrue("From customer, nation\nWhere c_nationkey = n_nationkey)" in eq)
        self.assertTrue("From customer, nation, orders\nWhere c_nationkey = n_nationkey and c_custkey = o_custkey)" in eq)
        # self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
