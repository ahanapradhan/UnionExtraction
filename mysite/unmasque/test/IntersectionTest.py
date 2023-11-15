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
        # self.assertTrue(self.pipeline.correct)

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
        self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
