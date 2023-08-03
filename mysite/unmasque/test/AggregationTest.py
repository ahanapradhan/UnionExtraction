import datetime
import unittest

from mysite.unmasque.refactored.aggregation import Aggregation
from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.test import tpchSettings, queries


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_agg_Q1(self):
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

        filter_predicates = [('lineitem', 'l_shipdate', '<=', datetime.date(1998, 9, 21), datetime.date(9999, 12, 31))]

        join_graph = []

        projections = ['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount'
            , 'l_tax', 'l_quantity', 'l_extendedprice', 'l_discount', '']

        group_by_attribs = ['l_returnflag', 'l_linestatus']
        has_groupBy = True

        global_key_attribs = []
        agg = Aggregation(self.conn, global_key_attribs,
                          global_attrib_types,
                          from_rels,
                          filter_predicates,
                          global_all_attribs, join_graph,
                          projections, has_groupBy, group_by_attribs)

        check = agg.doJob(queries.Q1)
        self.assertTrue(check)
        print(agg.global_aggregated_attributes)

        self.assertEqual(10, len(agg.global_aggregated_attributes))

        count = 0
        sum_count = 0
        avg_count = 0
        for agtuple in agg.global_aggregated_attributes:
            if agtuple[1] != '':
                count += 1
            if agtuple[1] == 'Sum':
                sum_count += 1
            elif agtuple[1] == 'Avg':
                avg_count += 1
        self.assertEqual(count, 8)
        self.assertEqual(sum_count, 4)
        self.assertEqual(avg_count, 3)

        self.conn.closeConnection()

    def test_agg_Q3(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3']

        filter_predicates = [('customer', 'c_mktsegment', 'equal', 'BUILDING', 'BUILDING'),
                             ('orders', 'o_orderdate', '<=', datetime.date(1, 1, 1), datetime.date(1995, 3, 14)),
                             ('lineitem', 'l_shipdate', '>=', datetime.date(1995, 3, 16), datetime.date(9999, 12, 31))]

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

        projections = ['l_orderkey', 'l_discount', 'o_orderdate', 'o_shippriority']

        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey', ]
        has_groupBy = True
        group_by_attribs = ['l_orderkey', 'o_orderdate', 'o_shippriority']

        agg = Aggregation(self.conn, global_key_attribs,
                          global_attrib_types,
                          from_rels,
                          filter_predicates,
                          global_all_attribs, join_graph,
                          projections, has_groupBy, group_by_attribs)

        check = agg.doJob(queries.Q3)
        self.assertTrue(check)
        print(agg.global_aggregated_attributes)

        self.assertEqual(4, len(agg.global_aggregated_attributes))

        count = 0
        sum_count = 0
        avg_count = 0
        for agtuple in agg.global_aggregated_attributes:
            if agtuple[1] != '':
                count += 1
            if agtuple[1] == 'Sum':
                sum_count += 1
            elif agtuple[1] == 'Avg':
                avg_count += 1
        self.assertEqual(count, 1)
        self.assertEqual(sum_count, 1)
        self.assertEqual(avg_count, 0)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
