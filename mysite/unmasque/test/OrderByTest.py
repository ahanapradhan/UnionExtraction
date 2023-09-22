import datetime
import unittest
import sys

sys.path.append("../../../")

from mysite.unmasque.refactored.orderby_clause import OrderBy
from mysite.unmasque.test.util import tpchSettings, queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_something(self):
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

        projections = ['l_orderkey', 'l_discount', 'o_totalprice', 'o_shippriority']
        names = ['orderkey', 'revenue', 'totalprice', 'shippriority']

        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey', ]

        global_aggregated_attributes = [('l_orderkey', ''), ('l_discount', 'Sum'), ('o_totalprice', 'Sum'),
                                        ('o_shippriority', '')]
        dep = [[('identical_expr_nc', 'o_orderkey')], [('identical_expr_nc', 'l_discount')], [('identical_expr_nc', 'o_totalprice')], [('identical_expr_nc', 'o_shippriority')]]
        ob = OrderBy(self.conn, global_key_attribs, global_attrib_types,
                     from_rels,
                     filter_predicates,
                     global_all_attribs,
                     join_graph,
                     projections,
                     names,
                     dep,
                     global_aggregated_attributes)
        check = ob.doJob(queries.Q3)
        print("Order by ", ob.orderBy_string)
        self.assertTrue(check)
        self.assertTrue(ob.has_orderBy)

        self.conn.closeConnection()
    
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
        projections = ['o_orderkey', 'l_quantity+l_extendedprice-1.0*l_extendedprice*l_discount', 'o_orderdate', 'o_shippriority']
        names = ['orderkey', 'revenue', 'orderdate', 'shippriority']
        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey', ]
        has_groupBy = True
        group_by_attribs = ['l_orderkey', 'o_orderdate', 'o_shippriority']
        dep = [[('identical_expr_nc', 'o_orderkey')], [('lineitem', 'l_quantity'), ('lineitem', 'l_extendedprice'), ('lineitem', 'l_discount')], [('identical_expr_nc', 'o_orderdate')], [('identical_expr_nc', 'o_shippriority')]]
        sol = [[], [[ 1.],[ 1.],[ 0.],[ 0.],[-1.],[-0.],[-0.],[ 0.]], [], []]
        p_list = [[], ['l_quantity', 'l_extendedprice', 'l_discount', 'l_quantity*l_extendedprice', 'l_extendedprice*l_discount', 'l_discount*l_quantity', 'l_quantity*l_extendedprice*l_discount'], [], []]
        global_aggregated_attributes = [('l_orderkey', ''), ('l_quantity+l_extendedprice-1.0*l_extendedprice*l_discount', 'Sum'), ('o_orderdate', ''),
                                        ('o_shippriority', '')]
        ob = OrderBy(self.conn, global_key_attribs, global_attrib_types,
                     from_rels,
                     filter_predicates,
                     global_all_attribs,
                     join_graph,
                     projections,
                     names,
                     dep,
                     global_aggregated_attributes)
        check = ob.doJob(queries.Q3_1)
        print("Order by ", ob.orderBy_string)
        self.assertTrue(check)
        self.assertTrue(ob.has_orderBy)

        self.conn.closeConnection()



if __name__ == '__main__':
    unittest.main()
