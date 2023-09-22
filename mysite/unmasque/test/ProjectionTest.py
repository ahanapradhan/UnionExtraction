import datetime
import sys
import unittest
sys.path.append("../../../")
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from mysite.unmasque.refactored.projection import Projection
from mysite.unmasque.test.util import tpchSettings, queries


class MyTestCase(BaseTestCase):

    def test_projection_Q1(self):
        self.conn.connectUsingParams()
        from_rels = tpchSettings.from_rels['Q1']

        global_attrib_types = [('lineitem', 'l_orderkey', 'integer'),
                               ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'),
                               ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'),
                               ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'),
                               ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'),
                               ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'),
                               ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'),
                               ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')]

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        filter_predicates = [('lineitem', 'l_shipdate', '<=', datetime.date(1, 1, 1),  datetime.date(1998, 9, 21))]

        join_graph = []

        pj = Projection(self.conn, global_attrib_types,
                        from_rels,
                        filter_predicates,
                        join_graph,
                        global_all_attribs)

        check = pj.doJob(queries.Q1)
        self.assertTrue(check)

        self.assertEqual(len(pj.projected_attribs), 10)
        self.assertEqual(len(pj.projection_names), 10)

        self.assertTrue('l_returnflag' in pj.projection_names)
        self.assertTrue('l_linestatus' in pj.projection_names)
        self.assertTrue('sum_qty' in pj.projection_names)
        self.assertTrue('sum_base_price' in pj.projection_names)
        self.assertTrue('sum_disc_price' in pj.projection_names)
        self.assertTrue('sum_charge' in pj.projection_names)
        self.assertTrue('avg_qty' in pj.projection_names)
        self.assertTrue('avg_price' in pj.projection_names)
        self.assertTrue('avg_disc' in pj.projection_names)
        self.assertTrue('count_order' in pj.projection_names)

        self.assertTrue('l_returnflag' in pj.projected_attribs)
        self.assertTrue('l_linestatus' in pj.projected_attribs)
        self.assertTrue('l_quantity' in pj.projected_attribs)
        self.assertTrue('l_extendedprice' in pj.projected_attribs)
        self.assertTrue('l_discount' in pj.projected_attribs)
        self.assertTrue('l_tax' in pj.projected_attribs)
        self.assertTrue('l_quantity' in pj.projected_attribs)
        self.assertTrue('l_extendedprice' in pj.projected_attribs)
        self.assertTrue('l_discount' in pj.projected_attribs)
        self.assertTrue('' in pj.projected_attribs)

        self.conn.closeConnection()

    def test_projections_Q3(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3']

        filter_predicates = [('customer', 'c_mktsegment', 'equal', 'BUILDING', 'BUILDING'),
                             ('orders', 'o_orderdate', '<=', datetime.date(1, 1, 1), datetime.date(1995, 3, 14)),
                             ('lineitem', 'l_shipdate', '>=', datetime.date(1995, 3, 17), datetime.date(9999, 12, 31))]

        global_all_attribs = [
            ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
            ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
             'o_shippriority', 'o_comment'],
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        join_graph = [['c_custkey', 'o_custkey'], ['o_orderkey', 'l_orderkey']]

        global_attrib_types = [('customer', 'c_custkey', 'integer'),
                               ('customer', 'c_name', 'character varying'),
                               ('customer', 'c_address', 'character varying'),
                               ('customer', 'c_nationkey', 'integer'),
                               ('customer', 'c_phone', 'character'),
                               ('customer', 'c_acctbal', 'numeric'),
                               ('customer', 'c_mktsegment', 'character'),
                               ('customer', 'c_comment', 'character varying'),
                               ('orders', 'o_orderkey', 'integer'),
                               ('orders', 'o_custkey', 'integer'),
                               ('orders', 'o_orderstatus', 'character'),
                               ('orders', 'o_totalprice', 'numeric'),
                               ('orders', 'o_orderdate', 'date'),
                               ('orders', 'o_orderpriority', 'character'),
                               ('orders', 'o_clerk', 'character'),
                               ('orders', 'o_shippriority', 'integer'),
                               ('orders', 'o_comment', 'character varying'),
                               ('lineitem', 'l_orderkey', 'integer'),
                               ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'),
                               ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'),
                               ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'),
                               ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'),
                               ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'),
                               ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'),
                               ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')]

        pj = Projection(self.conn, global_attrib_types,
                        from_rels,
                        filter_predicates,
                        join_graph,
                        global_all_attribs)

        check = pj.doJob(queries.Q3)
        self.assertTrue(check)

        self.assertEqual(frozenset({'orderkey', 'revenue', 'totalprice', 'shippriority'}),
                         frozenset(set(pj.projection_names)))
        self.assertEqual(frozenset({'o_orderkey', 'l_discount', 'o_totalprice', 'o_shippriority'}),
                         frozenset(set(pj.projected_attribs)))

        self.conn.closeConnection()

    def test_proj_Q3_1(self):
        for i in range(10):
            self.test_projections_Q3_1()

    def test_projections_Q3_1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3_1']

        filter_predicates = [('customer', 'c_mktsegment', 'equal', 'BUILDING', 'BUILDING'),
                             ('orders', 'o_orderdate', '<=', datetime.date(1, 1, 1), datetime.date(1995, 3, 14)),
                             ('lineitem', 'l_shipdate', '>=', datetime.date(1995, 3, 17), datetime.date(9999, 12, 31))]

        global_all_attribs = [
            ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
            ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
             'o_shippriority', 'o_comment'],
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        join_graph = [['c_custkey', 'o_custkey'], ['o_orderkey', 'l_orderkey']]

        global_attrib_types = [('customer', 'c_custkey', 'integer'),
                               ('customer', 'c_name', 'character varying'),
                               ('customer', 'c_address', 'character varying'),
                               ('customer', 'c_nationkey', 'integer'),
                               ('customer', 'c_phone', 'character'),
                               ('customer', 'c_acctbal', 'numeric'),
                               ('customer', 'c_mktsegment', 'character'),
                               ('customer', 'c_comment', 'character varying'),
                               ('orders', 'o_orderkey', 'integer'),
                               ('orders', 'o_custkey', 'integer'),
                               ('orders', 'o_orderstatus', 'character'),
                               ('orders', 'o_totalprice', 'numeric'),
                               ('orders', 'o_orderdate', 'date'),
                               ('orders', 'o_orderpriority', 'character'),
                               ('orders', 'o_clerk', 'character'),
                               ('orders', 'o_shippriority', 'integer'),
                               ('orders', 'o_comment', 'character varying'),
                               ('lineitem', 'l_orderkey', 'integer'),
                               ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'),
                               ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'),
                               ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'),
                               ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'),
                               ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'),
                               ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'),
                               ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')]

        pj = Projection(self.conn, global_attrib_types,
                        from_rels,
                        filter_predicates,
                        join_graph,
                        global_all_attribs)

        check = pj.doJob(queries.Q3_1)
        self.assertTrue(check)

        self.assertEqual(frozenset({'o_orderkey', 'l_quantity+l_extendedprice-1.0*l_extendedprice*l_discount', 'o_orderdate', 'o_shippriority'}),
                         frozenset(set(pj.projected_attribs)))

        self.conn.closeConnection()

    def test_projection_Q4(self):
        self.conn.connectUsingParams()
        print("Q4")
        from_rels = tpchSettings.from_rels['Q4']
        filter_predicates = [('orders', 'o_orderdate', '<=', datetime.date(1997, 7, 1), datetime.date(1997, 10, 1))]

        global_all_attribs = [
            ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
             'o_shippriority', 'o_comment']]

        join_graph = []

        global_attrib_types = [('orders', 'o_orderkey', 'integer'),
                               ('orders', 'o_custkey', 'integer'),
                               ('orders', 'o_orderstatus', 'character'),
                               ('orders', 'o_totalprice', 'numeric'),
                               ('orders', 'o_orderdate', 'date'),
                               ('orders', 'o_orderpriority', 'character'),
                               ('orders', 'o_clerk', 'character'),
                               ('orders', 'o_shippriority', 'integer'),
                               ('orders', 'o_comment', 'character varying')]

        pj = Projection(self.conn, global_attrib_types,
                        from_rels,
                        filter_predicates,
                        join_graph,
                        global_all_attribs)

        check = pj.doJob(queries.Q4)
        self.assertTrue(check)

        self.assertEqual(frozenset({'o_orderdate', 'o_orderpriority', 'order_count'}),
                         frozenset(set(pj.projection_names)))
        print(pj.projected_attribs)

        self.assertTrue('o_orderdate' in pj.projected_attribs)
        self.assertTrue('o_orderpriority' in pj.projected_attribs)

        self.conn.closeConnection()

    def test_projection_Q5(self):
        self.conn.connectUsingParams()
        from_rels = tpchSettings.from_rels['Q5']

        global_attrib_types = [('customer', 'c_custkey', 'integer'),
                               ('customer', 'c_name', 'character varying'),
                               ('customer', 'c_address', 'character varying'),
                               ('customer', 'c_nationkey', 'integer'),
                               ('customer', 'c_phone', 'character'),
                               ('customer', 'c_acctbal', 'numeric'),
                               ('customer', 'c_mktsegment', 'character'),
                               ('customer', 'c_comment', 'character varying'),
                               ('orders', 'o_orderkey', 'integer'),
                               ('orders', 'o_custkey', 'integer'),
                               ('orders', 'o_orderstatus', 'character'),
                               ('orders', 'o_totalprice', 'numeric'),
                               ('orders', 'o_orderdate', 'date'),
                               ('orders', 'o_orderpriority', 'character'),
                               ('orders', 'o_clerk', 'character'),
                               ('orders', 'o_shippriority', 'integer'),
                               ('orders', 'o_comment', 'character varying'),
                               ('lineitem', 'l_orderkey', 'integer'),
                               ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'),
                               ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'),
                               ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'),
                               ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'),
                               ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'),
                               ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'),
                               ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying'),
                               ('supplier', 's_suppkey', 'integer'),
                               ('supplier', 's_name', 'character'),
                               ('supplier', 's_address', 'character varying'),
                               ('supplier', 's_nationkey', 'integer'),
                               ('supplier', 's_phone', 'character'),
                               ('supplier', 's_acctbal', 'numeric'),
                               ('supplier', 's_comment', 'character varying'),
                               ('nation', 'n_nationkey', 'integer'),
                               ('nation', 'n_name', 'character'),
                               ('nation', 'n_regionkey', 'integer'),
                               ('nation', 'n_comment', 'character varying'),
                               ('region', 'r_regionkey', 'integer'),
                               ('region', 'r_name', 'character'),
                               ('region', 'r_comment', 'character varying')]

        global_all_attribs = [
            ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
            ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
             'o_shippriority', 'o_comment'],
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment'],
            ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'],
            ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'],
            ['r_regionkey', 'r_name', 'r_comment']]

        filter_predicates = [('orders', 'o_orderdate', '<=', datetime.date(1994, 1, 1), datetime.date(1995, 2, 28)),
                             ('region', 'r_name', 'equal', 'MIDDLE EAST', 'MIDDLE EAST')]

        join_graph = [['c_custkey', 'o_custkey'],
                      ['l_orderkey', 'o_orderkey'],
                      ['l_suppkey', 's_suppkey'],
                      ['c_nationkey', 's_nationkey', 'n_nationkey'],
                      ['n_regionkey', 'r_regionkey']]

        pj = Projection(self.conn, global_attrib_types,
                        from_rels,
                        filter_predicates,
                        join_graph,
                        global_all_attribs)

        check = pj.doJob(queries.Q5)
        self.assertTrue(check)

        self.assertEqual(len(pj.projected_attribs), 2)
        self.assertEqual(len(pj.projection_names), 2)

        self.assertTrue('n_name' in pj.projection_names)
        self.assertTrue('revenue' in pj.projection_names)
        self.assertTrue('n_name' in pj.projected_attribs)
        self.assertTrue('l_extendedprice' in pj.projected_attribs)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
