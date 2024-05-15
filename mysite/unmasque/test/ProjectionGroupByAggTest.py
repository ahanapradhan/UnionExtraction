import datetime
import unittest

import pytest

from mysite.unmasque.src.core.aggregation import Aggregation
from mysite.unmasque.src.core.groupby_clause import GroupBy
from mysite.unmasque.src.core.projection import Projection
from mysite.unmasque.src.util.constants import IDENTICAL_EXPR, SUM, AVG, MIN, MAX, COUNT
from mysite.unmasque.test.util import queries, tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    @pytest.mark.skip
    def test_in_loop(self):
        for i in range(2):
            self.test_something()

    @pytest.mark.skip
    def test_something(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        query = queries.Q3_1

        from_rels = tpchSettings.from_rels['Q3_1']

        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey', ]

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
        global_min_instance_dict = {}

        pj = Projection(self.conn, delivery)
        pj.mock = True

        self.conn.execute_sql(["alter table customer rename to customer_copy;",
                               "create table customer (like customer_copy);",
                               f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
                               f"c_mktsegment,c_comment)"
                               f"VALUES (136777,\'Customer#000060217\',\'kolkata\',2,\'27-299-23-31\',4089.02,"
                               f"\'BUILDING\',\'Nothing\');",

                               "alter table lineitem rename to lineitem_copy;",
                               "create table lineitem (like lineitem_copy);",
                               f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
                               f"l_extendedprice,l_discount,"
                               f"l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,"
                               f"l_shipmode,l_comment) "
                               f"VALUES (136777,136777,136777,2,25.0,4089.02,10.0,12.02,\'A\',\'F\',\'1997-01-01\',"
                               f"\'1995-01-01\',\'1995-01-01\',\'COD COLLECT\',\'AIR\',\'Nothing\');",

                               "alter table orders rename to orders_copy;",
                               "create table orders (like orders_copy);",
                               f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
                               f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
                               f"VALUES (136777,136777,\'N\',9991.32,\'1993-01-01\', \'URGENT\', \'clerk#0000001\',"
                               f"0,\'hello world bye bye\');",
                               ])

        check = pj.doJob(query)
        self.assertTrue(check)

        self.assertEqual(frozenset({'o_orderkey', 'l_quantity+l_extendedprice-1.0*l_extendedprice*l_discount'
                                       , 'o_orderdate', 'o_shippriority'}), frozenset(set(pj.projected_attribs)))

        gb = GroupBy(self.conn, delivery, pgao_ctx)
        gb.mock = True
        check = gb.doJob(query)
        self.assertTrue(check)

        self.assertTrue(gb.has_groupby)
        self.assertEqual(3, len(gb.group_by_attrib))
        self.assertTrue('o_orderkey' in gb.group_by_attrib)
        self.assertTrue('o_orderdate' in gb.group_by_attrib)
        self.assertTrue('o_shippriority' in gb.group_by_attrib)

        self.assertEqual(len(pj.dependencies), 4)
        self.assertTrue([(IDENTICAL_EXPR, 'o_orderkey')] in pj.dependencies)
        self.assertTrue([(IDENTICAL_EXPR, 'o_shippriority')] in pj.dependencies)
        self.assertTrue([(IDENTICAL_EXPR, 'o_orderdate')] in pj.dependencies)
        self.assertTrue([('lineitem', 'l_quantity'), ('lineitem', 'l_extendedprice'),
                         ('lineitem', 'l_discount')] in pj.dependencies)
        print(pj.solution)
        v = [[1.], [1.], [0.], [-0.], [-0.], [-1.], [0.], [-0.]]
        sol = [[], v, [], []]
        empty_sol = 0
        for e in range(4):
            if not sol[e]:
                empty_sol += 1
            else:
                checkval = sol[e]
                val = pj.solution[e]
                self.assertEqual(len(checkval), len(val))
                for x in range(len(checkval)):
                    self.assertEqual(checkval[x], val[x])
        self.assertEqual(3, empty_sol)

        self.assertEqual(4, len(pj.param_list))
        empty_p = 0
        for p in pj.param_list:
            if not p:
                empty_p += 1
            else:
                self.assertEqual(p, ['l_quantity', 'l_extendedprice', 'l_discount', 'l_quantity*l_extendedprice',
                                     'l_quantity*l_discount',
                                     'l_extendedprice*l_discount', 'l_quantity*l_extendedprice*l_discount'])
                self.assertTrue('l_quantity' in p)
                self.assertTrue('l_extendedprice' in p)
                self.assertTrue('l_discount' in p)
                self.assertTrue('l_quantity*l_extendedprice' in p or 'l_extendedprice*l_quantity' in p)
                self.assertTrue('l_extendedprice*l_discount' in p or 'l_discount*l_extendedprice' in p)
                self.assertTrue('l_discount*l_quantity' in p or 'l_quantity*l_discount' in p)
                self.assertTrue('l_quantity*l_extendedprice*l_discount' in p
                                or 'l_extendedprice*l_quantity*l_dicsount' in p
                                or 'l_discount*l_quantity*l_extendedprice' in p
                                or 'l_discount*l_extendedprice*l_quantity' in p
                                or 'l_quantity*l_discount*l_extendedprice' in p
                                or 'l_extendedprice*l_discount*l_quantity' in p)
        self.assertEqual(3, empty_p)

        agg = Aggregation(self.conn, delivery)
        agg.mock = True

        check = agg.doJob(query)
        self.assertTrue(check)
        self.assertEqual(4, len(agg.global_aggregated_attributes))

        self.conn.execute_sql(["drop table customer;",
                               "drop table lineitem;",
                               "drop table orders;",
                               "alter table customer_copy to customer;",
                               "alter table lineitem_copy to lineitem;",
                               "alter table orders_copy to orders;"])

        count = 0
        sum_count = 0
        avg_count = 0
        for agtuple in agg.global_aggregated_attributes:
            if agtuple[1] != '':
                count += 1
            if agtuple[1] == SUM:
                sum_count += 1
            elif agtuple[1] == AVG:
                avg_count += 1
            elif agtuple[1] == MIN:
                avg_count += 1
            elif agtuple[1] == MAX:
                avg_count += 1
            elif agtuple[1] == COUNT:
                avg_count += 1
        self.assertEqual(count, 1)
        self.assertEqual(sum_count, 1)
        self.assertEqual(avg_count, 0)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
