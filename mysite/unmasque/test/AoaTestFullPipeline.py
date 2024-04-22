import copy
import datetime
import unittest

import pytest

from mysite.unmasque.src.core.cs2 import Cs2
from mysite.unmasque.src.core.elapsed_time import create_zero_time_profile
from mysite.unmasque.src.core.projection import Projection
from mysite.unmasque.src.core.view_minimizer import ViewMinimizer
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from ..src.core.aoa import AlgebraicPredicate
from ..test.util import tpchSettings
from ..test.util.BaseTestCase import BaseTestCase


def remove_transitive_relations(input_list):
    result = []

    def is_transitive(tuple1, tuple2):
        return tuple1[1] == tuple2[0]

    for i in range(len(input_list)):
        is_transitive_relation = any(is_transitive(input_list[i], j) for j in input_list)
        if not is_transitive_relation:
            result.append(input_list[i])

    return result


def get_subquery1():
    return "SELECT c_name as name, (c_acctbal - o_totalprice) as account_balance " \
           "FROM orders, customer, nation WHERE c_custkey = o_custkey " \
           "and c_nationkey = n_nationkey " \
           "and c_mktsegment = 'FURNITURE'" \
           "and n_name = 'INDIA' " \
           "and o_orderdate between '1998-01-01' and '1998-01-05' " \
           "and o_totalprice <= c_acctbal;", ['customer', 'orders', 'nation']


def get_subquery2():
    return "SELECT s_name as name, " \
           "(s_acctbal + o_totalprice) as account_balance " \
           "FROM supplier, lineitem, orders, nation " \
           "WHERE l_suppkey = s_suppkey " \
           "and l_orderkey = o_orderkey " \
           "and s_nationkey = n_nationkey and n_name = 'ARGENTINA' " \
           "and o_orderdate between '1998-01-01' and '1998-01-05' " \
           "and o_totalprice >= s_acctbal and o_totalprice >= 30000 " \
           "and 50000 > s_acctbal;", ['orders', 'lineitem', 'supplier', 'nation']


class MyTestCase(BaseTestCase):
    global_min_instance_dict = None

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_dormant_aoa(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode, count(*) as count From orders, lineitem " \
                "Where o_orderkey = l_orderkey and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate " \
                "and l_receiptdate >= '1994-01-01' and l_receiptdate <= '1995-01-01' and l_extendedprice <= " \
                "o_totalprice and l_extendedprice <= 70000 and o_totalprice >= 60000 Group By l_shipmode " \
                "Order By l_shipmode;"
        core_rels = ['orders', 'lineitem']
        aoa, check = self.run_pipeline(core_rels, query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(aoa.where_clause.count("and"), 7)
        self.conn.closeConnection()

    def test_multiple_aoa_1(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem', 'partsupp']
        query = "Select l_quantity, l_shipinstruct From orders, lineitem, partsupp " \
                "Where ps_partkey = l_partkey " \
                "and ps_suppkey = l_suppkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate " \
                "and ps_availqty <= l_orderkey " \
                "and l_extendedprice <= 20000 " \
                "and o_totalprice < 60005 " \
                "and ps_supplycost < 500 " \
                "and l_linenumber < 2 " \
                "Order By l_orderkey Limit 10;"
        aoa, check = self.run_pipeline(core_rels, query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(aoa.where_clause.count("and"), 8)
        self.assertEqual(aoa.where_clause.count("<="), 6)
        self.assertEqual(aoa.where_clause.count(" ="), 3)
        self.conn.closeConnection()

    def test_non_key_aoa_join(self):
        self.conn.connectUsingParams()
        core_rels = ['lineitem', 'partsupp']
        query = "Select l_shipmode From lineitem, partsupp " \
                "Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber " \
                "Group By l_shipmode " \
                "Order By l_shipmode Limit 5;"
        aoa, check = self.run_pipeline(core_rels, query)
        self.assertTrue(check)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 3)
        print(aoa.where_clause)

    def test_date_predicate_aoa(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem']
        query = "Select o_orderpriority, count(*) as order_count " \
                "From orders, lineitem Where l_orderkey = o_orderkey " \
                "and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01'" \
                " and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority;"

        aoa, check = self.run_pipeline(core_rels, query)
        self.assertTrue(check)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 1)
        self.assertTrue(('lineitem', 'l_orderkey') in aoa.algebraic_eq_predicates[0])
        self.assertTrue(('orders', 'o_orderkey') in aoa.algebraic_eq_predicates[0])

        self.assertEqual(len(aoa.aoa_predicates), 3)
        self.assertTrue((('lineitem', 'l_commitdate'), ('lineitem', 'l_receiptdate')) in aoa.aoa_predicates)
        self.assertTrue([datetime.date(1993, 7, 1), ('orders', 'o_orderdate')] in aoa.aoa_predicates)
        self.assertTrue([('orders', 'o_orderdate'), datetime.date(1993, 9, 30)] in aoa.aoa_predicates)
        self.conn.closeConnection()

    def run_pipeline(self, core_rels, query):
        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings.key_lists)
        cs2.iteration_count = 0
        check = cs2.doJob(query)
        self.assertTrue(cs2.done)
        vm = ViewMinimizer(self.conn, core_rels, cs2.sizes, cs2.passed)
        check = vm.doJob(query)
        self.assertTrue(vm.done and check)
        self.global_min_instance_dict = copy.deepcopy(vm.global_min_instance_dict)
        aoa = AlgebraicPredicate(self.conn, core_rels, pending_predicates, filter_extractor,
                                 self.global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.global_min_instance_dict = copy.deepcopy(vm.global_min_instance_dict)
        return aoa, check

    def run_pipeline_till_projection(self, core_rels, query):
        aoa, check = self.run_pipeline(core_rels, query)
        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)

        check = pj.doJob(query)
        self.assertTrue(check)
        return aoa, check, pj

    @pytest.mark.skip
    def test_paper_subquery1(self):
        self.conn.connectUsingParams()
        query, from_rels = get_subquery1()

        self.assertTrue(self.conn.conn is not None)
        aoa, check = self.run_pipeline(from_rels, query)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        print(aoa.filter_extractor.filter_predicates)
        print(aoa.aoa_predicates)
        print(aoa.where_clause)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_paper_subquery2(self):
        self.conn.connectUsingParams()
        query, from_rels = get_subquery2()
        self.assertTrue(self.conn.conn is not None)
        aoa, check = self.run_pipeline(from_rels, query)
        self.assertTrue(check)
        print(aoa.filter_extractor.filter_predicates)
        print(aoa.aoa_predicates)
        print(aoa.filter_extractor.global_min_instance_dict)
        print(aoa.where_clause)
        self.conn.closeConnection()

    def test_UQ11(self):
        self.conn.connectUsingParams()
        query = "Select o_orderpriority, " \
                "count(*) as order_count " \
                "From orders, lineitem " \
                "Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' " \
                "and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate " \
                "Group By o_orderpriority " \
                "Order By o_orderpriority;"
        from_rels = ['orders', 'lineitem']
        self.assertTrue(self.conn.conn is not None)
        aoa, check = self.run_pipeline(from_rels, query)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        print(aoa.filter_predicates)
        print(aoa.aoa_predicates)
        print(aoa.where_clause)
        self.conn.closeConnection()

    def test_UQ10(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate;"

        from_rels = ['orders', 'lineitem']
        self.assertTrue(self.conn.conn is not None)
        aoa, check, pj = self.run_pipeline_till_projection(from_rels, query)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertTrue("lineitem.l_shipdate < lineitem.l_commitdate" in aoa.where_clause)
        self.assertTrue("lineitem.l_commitdate < lineitem.l_receiptdate" in aoa.where_clause)
        self.conn.closeConnection()
        print(pj.projected_attribs)
        print(pj.projection_names)

    def test_UQ13(self):
        self.conn.connectUsingParams()
        query = "Select l_orderkey, l_linenumber " \
                "From orders, lineitem, partsupp " \
                "Where o_orderkey = l_orderkey " \
                "and ps_partkey = l_partkey " \
                "and ps_suppkey = l_suppkey " \
                "and ps_availqty = l_linenumber " \
                "and l_shipdate >= o_orderdate " \
                "and o_orderdate >= '1990-01-01' " \
                "and l_commitdate <= l_receiptdate " \
                "and l_shipdate <= l_commitdate " \
                "and l_receiptdate > '1994-01-01' " \
                "Order By l_orderkey Limit 7;"

        from_rels = ['orders', 'lineitem', 'partsupp']
        self.assertTrue(self.conn.conn is not None)
        aoa, check, pj = self.run_pipeline_till_projection(from_rels, query)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        print(aoa.where_clause)
        print(pj.projected_attribs)
        print(pj.projection_names)
        self.assertTrue("orders.o_orderdate <= lineitem.l_shipdate" in aoa.where_clause)
        self.assertTrue("lineitem.l_commitdate <= lineitem.l_receiptdate" in aoa.where_clause)
        self.assertTrue("lineitem.l_shipdate <= lineitem.l_commitdate" in aoa.where_clause)
        self.assertTrue("'1990-01-01' <= orders.o_orderdate" in aoa.where_clause)
        # self.assertEqual(8, aoa.where_clause.count("and"))
        self.conn.closeConnection()

    def test_UQ12(self):
        self.conn.connectUsingParams()
        query = "Select p_brand, o_clerk, l_shipmode " \
                "From orders, lineitem, part " \
                "Where l_partkey = p_partkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate " \
                "and o_orderdate > '1994-01-01' " \
                "and l_shipdate > '1995-01-01' " \
                "and p_retailprice >= l_extendedprice " \
                "and p_partkey < 10000 " \
                "and l_suppkey < 10000 " \
                "and p_container = 'LG CAN' " \
                "Order By l_orderkey LIMIT 10"

        from_rels = ['orders', 'lineitem', 'part']
        self.assertTrue(self.conn.conn is not None)
        aoa, check, pj = self.run_pipeline_till_projection(from_rels, query)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertTrue("lineitem.l_extendedprice <= part.p_retailprice" in aoa.where_clause)
        self.assertTrue("orders.o_orderdate <= lineitem.l_shipdate" in aoa.where_clause)
        self.conn.closeConnection()
        print(pj.projected_attribs)
        print(pj.projection_names)

    def test_paper_subquery1_projection(self):
        self.conn.connectUsingParams()
        query, from_rels = get_subquery1()
        self.assertTrue(self.conn.conn is not None)
        aoa, check, pj = self.run_pipeline_till_projection(from_rels, query)
        # print(self.global_min_instance_dict)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(aoa.where_clause.count("and"), 6)
        self.assertTrue("customer.c_custkey = orders.o_custkey" in aoa.where_clause)
        self.assertTrue("customer.c_nationkey = nation.n_nationkey" in aoa.where_clause)
        self.assertTrue("customer.c_mktsegment = 'FURNITURE'" in aoa.where_clause)
        self.assertTrue("nation.n_name = 'INDIA'" in aoa.where_clause)
        self.assertTrue("orders.o_orderdate  >= '1998-01-01'" in aoa.where_clause)
        self.assertTrue("orders.o_orderdate <= '1998-01-05'" in aoa.where_clause)
        self.assertTrue("orders.o_totalprice <= customer.c_acctbal" in aoa.where_clause)

        self.assertEqual(len(pj.projected_attribs), 2)
        self.assertEqual(pj.projected_attribs[0], 'c_name')
        self.assertEqual(pj.projected_attribs[1], 'c_acctbal - o_totalprice')
        self.conn.closeConnection()

    def test_paper_subquery2_projection(self):
        self.conn.connectUsingParams()
        query, from_rels = get_subquery2()
        self.assertTrue(self.conn.conn is not None)
        aoa, check, pj = self.run_pipeline_till_projection(from_rels, query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(aoa.where_clause.count("and"), 8)
        self.assertTrue("lineitem.l_suppkey = supplier.s_suppkey" in aoa.where_clause)
        self.assertTrue("lineitem.l_orderkey = orders.o_orderkey" in aoa.where_clause)
        self.assertTrue("nation.n_nationkey = supplier.s_nationkey" in aoa.where_clause)
        self.assertTrue("nation.n_name = 'ARGENTINA'" in aoa.where_clause)
        self.assertTrue("orders.o_orderdate  >= '1998-01-01'" in aoa.where_clause)
        self.assertTrue("orders.o_orderdate <= '1998-01-05'" in aoa.where_clause)
        self.assertTrue("supplier.s_acctbal <= orders.o_totalprice" in aoa.where_clause)
        # self.assertTrue("30000 <= o_totalprice" in aoa.where_clause)
        # self.assertTrue("s_acctbal <= 50000" in aoa.where_clause)

        self.assertEqual(len(pj.projected_attribs), 2)
        self.assertEqual(pj.projected_attribs[0], 's_name')
        self.assertEqual(pj.projected_attribs[1], 'o_totalprice + s_acctbal')
        self.conn.closeConnection()

    def test_big_join_aoa(self):
        self.conn.connectUsingParams()
        query = f"Select o_orderstatus, l_shipmode From lineitem, orders, customer, nation " \
                f"Where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey and " \
                f"l_linenumber >= 4 and n_name LIKE 'IND%' " \
                f"and c_acctbal < l_extendedprice and l_extendedprice < o_totalprice;"
        aoa, check, pj = self.run_pipeline_till_projection(['orders', 'lineitem', 'customer', 'nation'], query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(aoa.where_clause.count("and"), 6)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
