import datetime
import unittest

from mysite.unmasque.refactored.aoa import AlgebraicPredicate
from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.refactored.view_minimizer import ViewMinimizer
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_multiple_aoa(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem', 'partsupp']
        query = "Select l_quantity, l_shipinstruct From orders, lineitem, partsupp " \
                "Where ps_partkey = l_partkey " \
                "and ps_suppkey = l_suppkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate " \
                "and ps_availqty <= l_orderkey " \
                "and l_extendedprice <= 20000 " \
                "and o_totalprice <= 60000 " \
                "and ps_supplycost <= 500 " \
                "and l_linenumber = 1 " \
                "Order By l_orderkey Limit 10;"
        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings.key_lists)
        cs2.iteration_count = 0
        check = cs2.doJob(query)
        self.assertTrue(cs2.done)

        vm = ViewMinimizer(self.conn, core_rels, cs2.sizes, cs2.passed)
        check = vm.doJob(query)
        self.assertTrue(vm.done and check)

        aoa = AlgebraicPredicate(self.conn, tpchSettings.key_lists, core_rels, vm.global_min_instance_dict)
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)

        self.conn.closeConnection()

    def test_date_predicate_aoa(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem']
        query = "Select o_orderpriority, count(*) as order_count " \
                "From orders, lineitem Where l_orderkey = o_orderkey " \
                "and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01'" \
                " and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority;"

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings.key_lists)
        cs2.iteration_count = 0
        check = cs2.doJob(query)
        self.assertTrue(cs2.done)

        vm = ViewMinimizer(self.conn, core_rels, cs2.sizes, cs2.passed)
        check = vm.doJob(query)
        self.assertTrue(vm.done and check)

        aoa = AlgebraicPredicate(self.conn, tpchSettings.key_lists, core_rels, vm.global_min_instance_dict)
        check = aoa.doJob(query)
        self.assertTrue(check)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 1)
        self.assertTrue(('lineitem', 'l_orderkey') in aoa.algebraic_eq_predicates[0])
        self.assertTrue(('orders', 'o_orderkey') in aoa.algebraic_eq_predicates[0])

        self.assertEqual(len(aoa.aoa_predicates), 3)
        self.assertTrue((('lineitem', 'l_commitdate'), ('lineitem', 'l_receiptdate')) in aoa.aoa_predicates)
        self.assertTrue([datetime.date(1993, 7, 1), ('orders', 'o_orderdate')] in aoa.aoa_predicates)
        self.assertTrue([('orders', 'o_orderdate'), datetime.date(1993, 9, 30)] in aoa.aoa_predicates)
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
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq)
        # self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
