import unittest

from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.ExtractionTest import set_optimizer_params
from mysite.unmasque.test.util import queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from results.tpch_kapil_report import Q3


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_extract_Q3_optimizer_options_on(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(set_optimizer_params(False))
        key = 'Q3'
        query = queries.queries_dict[key]
        query = Q3
        query = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " \
                "From customer, orders, lineitem " \
                "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
                "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " \
                "Group By l_orderkey, o_totalprice, o_shippriority " \
                "Order by revenue desc, o_totalprice Limit 10;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
