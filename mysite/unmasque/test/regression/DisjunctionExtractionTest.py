import unittest

from ...test.util.BaseTestCase import BaseTestCase
from ...src.pipeline.ExtractionPipeLine import ExtractionPipeLine


class DisjunctionTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = True
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_sumang_thesis_Q2(self):
        query = "select c_mktsegment,MAX(c_acctbal) from customer where c_nationkey IN (1,5,9,10) group by " \
                "c_mktsegment;"

        eq = self.pipeline.extract(query)
        print(eq)
        self.assertEqual(eq.count(" IN "), 1)
        # self.assertTrue(self.pipeline.correct)

    def test_sumang_thesis_Q3(self):
        query = "select l_shipmode,sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and (l_quantity =42 or l_quantity =50 or l_quantity=24) group by l_shipmode limit 100;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_sumang_thesis_Q4(self):
        query = "select AVG(l_extendedprice) as avgTOTAL from lineitem,part " \
                "where p_partkey = l_partkey and (p_brand = 'Brand#52' or p_brand = 'Brand#12') and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE') ORDER BY avgTOTAL desc LIMIT 50;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

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


if __name__ == '__main__':
    unittest.main()
