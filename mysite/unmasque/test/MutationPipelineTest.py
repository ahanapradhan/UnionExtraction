import datetime
import unittest

from mysite.unmasque.src.core.elapsed_time import create_zero_time_profile
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.AoaTestFullPipeline import get_subquery1
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.use_cs2 = False
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_multiple_aoa(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem', 'partsupp']
        query = "Select l_quantity, l_shipinstruct From orders, lineitem, partsupp " \
                "Where ps_partkey = l_partkey " \
                "and ps_suppkey = l_suppkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate; "
        """
                "and ps_availqty <= l_orderkey " \
                "and l_extendedprice <= 20000 " \
                "and o_totalprice <= 60000 " \
                "and ps_supplycost <= 500 " \
                "and l_linenumber = 1 " \
                "Order By l_orderkey Limit 10;"
        """
        time_profile = create_zero_time_profile()
        self.pipeline.all_sizes = tpchSettings.all_size
        aoa, time_profile = self.pipeline._mutation_pipeline(core_rels, query, time_profile)
        print(aoa.aoa_predicates)
        # self.assertEqual(len(aoa.aoa_predicates), 2)
        # self.assertTrue((('orders', 'o_orderdate'), ('lineitem', 'l_shipdate')) in aoa.aoa_predicates)
        # self.assertTrue((('partsupp', 'ps_availqty'), ('lineitem', 'l_orderkey')) in aoa.aoa_predicates)
        print(aoa.aoa_less_thans)
        # self.assertFalse(len(aoa.aoa_less_thans))

        print(aoa.arithmetic_ineq_predicates)
        # self.assertEqual(len(aoa.arithmetic_ineq_predicates), 2)

        print(aoa.algebraic_eq_predicates)
        # self.assertEqual(len(aoa.algebraic_eq_predicates), 3)
        print(aoa.arithmetic_eq_predicates)
        # self.assertEqual(len(aoa.arithmetic_eq_predicates), 1)
        # self.assertTrue(('lineitem', 'l_linenumber', '=', 1, 1) in aoa.arithmetic_eq_predicates)

        self.conn.closeConnection()

    def test_aoa_dev(self):
        query = "SELECT c_name as name, " \
                "c_acctbal as account_balance " \
                "FROM orders, customer, nation " \
                "WHERE o_custkey > 2500 and c_custkey = o_custkey and c_custkey <= 5000" \
                "and c_nationkey = n_nationkey " \
                "and o_orderdate between '1998-01-01' and '1998-01-15' " \
                "and o_totalprice <= c_acctbal;"
        self.conn.connectUsingParams()
        core_rels = ['orders', 'customer', 'nation']
        time_profile = create_zero_time_profile()
        self.pipeline.all_sizes = tpchSettings.all_size
        aoa, time_profile = self.pipeline._mutation_pipeline(core_rels, query, time_profile)

        print(aoa.aoa_predicates)
        self.assertEqual(len(aoa.aoa_predicates), 1)
        self.assertTrue((('orders', 'o_totalprice'), ('customer', 'c_acctbal')) in aoa.aoa_predicates)
        print(aoa.aoa_less_thans)
        self.assertFalse(len(aoa.aoa_less_thans))

        print(aoa.arithmetic_ineq_predicates)
        self.assertEqual(len(aoa.arithmetic_ineq_predicates), 2)

        print(aoa.algebraic_eq_predicates)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 2)
        print(aoa.arithmetic_eq_predicates)
        self.assertFalse(len(aoa.arithmetic_eq_predicates))
        self.conn.closeConnection()

    def test_paper_subquery1(self):
        self.conn.connectUsingParams()
        query, from_rels = get_subquery1()

        time_profile = create_zero_time_profile()
        self.pipeline.all_sizes = tpchSettings.all_size
        aoa, time_profile = self.pipeline._mutation_pipeline(from_rels, query, time_profile)

        print(aoa.aoa_predicates)
        self.assertEqual(len(aoa.aoa_predicates), 1)
        self.assertTrue((('orders', 'o_totalprice'), ('customer', 'c_acctbal')) in aoa.aoa_predicates)
        print(aoa.aoa_less_thans)
        self.assertFalse(len(aoa.aoa_less_thans))

        print(aoa.arithmetic_ineq_predicates)
        self.assertEqual(len(aoa.arithmetic_ineq_predicates), 1)
        self.assertTrue(
            ('orders', 'o_orderdate', 'range', datetime.date(1998, 1, 1),
             datetime.date(1998, 1, 5)) in aoa.arithmetic_ineq_predicates)

        print(aoa.algebraic_eq_predicates)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 2)
        print(aoa.arithmetic_eq_predicates)
        self.assertEqual(len(aoa.arithmetic_eq_predicates), 2)
        self.assertTrue(('customer', 'c_mktsegment', 'equal', 'FURNITURE', 'FURNITURE') in aoa.arithmetic_eq_predicates)
        self.assertTrue(('nation', 'n_name', 'equal', 'INDIA', 'INDIA') in aoa.arithmetic_eq_predicates)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
