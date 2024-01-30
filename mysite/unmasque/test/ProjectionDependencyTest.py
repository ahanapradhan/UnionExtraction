import datetime
import sys
import unittest

sys.path.append("../../../")
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from mysite.unmasque.refactored.projection import Projection


class MyTestCase(BaseTestCase):

    def test_projection_find_dependency_1(self):
        self.conn.connectUsingParams()
        q = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp1, supplier1, nation1 " \
            "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT " \
            "Order by value desc Limit 100;"
        core_rels = ['partsupp1', 'supplier1', 'nation1']

        join_graph = [['ps_suppkey', 's_suppkey'], ['s_nationkey', 'n_nationkey']]

        global_key_attribs = ['ps_partkey', 'ps_suppkey', 's_suppkey', 's_nationkey',
                              'n_nationkey', 'n_regionkey']

        filters = [('nation1', 'n_name', 'equal', 'ARGENTINA', 'ARGENTINA')]
        global_attrib_types = {('partsupp1', 'ps_partkey', 'integer'),
                               ('partsupp1', 'ps_suppkey', 'integer'),
                               ('partsupp1', 'ps_availqty', 'integer'),
                               ('partsupp1', 'ps_supplycost', 'numeric'),
                               ('partsupp1', 'ps_comment', 'character varying'),
                               ('supplier1', "s_suppkey", "integer"),
                               ('supplier1', "s_name", "character"),
                               ('supplier1', "s_address", "character varying"),
                               ('supplier1', "s_nationkey", "integer"),
                               ('supplier1', "s_phone", "character"),
                               ('supplier1', "s_acctbal", "numeric"),
                               ('supplier1', "s_comment", "character varying"),
                               ('nation1', "n_nationkey", "integer"),
                               ('nation1', "n_name", "character"),
                               ('nation1', "n_regionkey", "integer"),
                               ('nation1', "n_comment", "character varying")}
        global_all_attribs = [['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
                              ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
                              ["n_nationkey",
                               "n_name",
                               "n_regionkey",
                               "n_comment"]]
        global_min_instance_dict = {}
        pj = Projection(self.conn, global_attrib_types, core_rels, filters, join_graph, global_all_attribs,
                        global_min_instance_dict, aoa_predicates)
        pj.mock = True

        self.conn.execute_sql(["drop table if exists partsupp1;", "create table partsupp1 (like partsupp);",
                               f"Insert into partsupp1(ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment) "
                               f"VALUES (152257,4773,2,2.00,\'regular dependencies\');",

                               "drop table if exists supplier1;", "create table supplier1 (like supplier);",
                               f"Insert into supplier1(s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,"
                               f"s_comment) "
                               f"VALUES (4773,\'supplier#00123456\',\'kolkata\',5,\'530-27-279\',443.09,\'hello world "
                               f"bye bye\');",

                               "drop table if exists nation1;", "create table nation1 (like nation);",
                               f"Insert into nation1(n_nationkey,n_name,n_regionkey,n_comment) "
                               f"VALUES (5,\'ARGENTINA\',2,\'hello world bye bye\');",
                               ])

        pj.do_init()
        s_values = []
        attribs, names, deps, flag = pj.find_projection_dependencies(q, s_values)
        print("===========")
        print(attribs)
        print(deps)
        print("===========")

        self.assertEqual(len(attribs), 2)
        self.assertTrue('ps_comment' in attribs)
        self.assertTrue('' in attribs)
        self.assertEqual(len(deps), 2)
        for dep in deps:
            if len(dep) == 1:
                self.assertEqual(('partsupp1', 'ps_comment'), dep[0])
            if len(dep) == 2:
                self.assertTrue(('partsupp1', 'ps_supplycost') in dep)
                self.assertTrue(('partsupp1', 'ps_availqty') in dep)

        self.conn.execute_sql(["drop table if exists partsupp1;",
                               "drop table if exists supplier1;", "drop table if exists nation1;"])

        self.conn.closeConnection()

    def test_projection_find_dependency_2(self):
        self.conn.connectUsingParams()
        q = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " \
            "From customer1, orders1, lineitem1 " \
            "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
            "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " \
            "Group By l_orderkey, o_orderdate, o_shippriority " \
            "Order by revenue desc, o_orderdate Limit 10;"

        core_rels = ['customer1', 'lineitem1', 'orders1']
        global_key_attribs = ['c_custkey', 'c_nationkey', 'l_orderkey', 'l_partkey', 'l_suppkey',
                              'o_orderkey', 'o_custkey']

        global_attrib_types = {('customer1', 'c_custkey', 'integer'),
                               ('customer1', 'c_name', 'character varying'),
                               ('customer1', 'c_address', 'character varying'),
                               ('customer1', 'c_nationkey', 'integer'),
                               ('customer1', 'c_phone', 'character'),
                               ('customer1', 'c_acctbal', 'numeric'),
                               ('customer1', 'c_mktsegment', 'character'),
                               ('customer1', 'c_comment', 'character varying'),
                               ('lineitem1', 'l_orderkey', 'integer'), ('lineitem1', 'l_partkey', 'integer'),
                               ('lineitem1', 'l_suppkey', 'integer'), ('lineitem1', 'l_linenumber', 'integer'),
                               ('lineitem1', 'l_quantity', 'numeric'), ('lineitem1', 'l_extendedprice', 'numeric'),
                               ('lineitem1', 'l_discount', 'numeric'), ('lineitem1', 'l_tax', 'numeric'),
                               ('lineitem1', 'l_returnflag', 'character'), ('lineitem1', 'l_linestatus', 'character'),
                               ('lineitem1', 'l_shipdate', 'date'), ('lineitem1', 'l_commitdate', 'date'),
                               ('lineitem1', 'l_receiptdate', 'date'), ('lineitem1', 'l_shipinstruct', 'character'),
                               ('lineitem1', 'l_shipmode', 'character'),
                               ('lineitem1', 'l_comment', 'character varying'),
                               ('orders1', "o_orderkey", "integer"),
                               ('orders1', "o_custkey", "integer"),
                               ('orders1', "o_orderstatus", "character"),
                               ('orders1', "o_totalprice", "numeric"),
                               ('orders1', "o_orderdate", "date"),
                               ('orders1', "o_orderpriority", "character"),
                               ('orders1', "o_clerk", "character"),
                               ('orders1', "o_shippriority", "integer"),
                               ('orders1', "o_comment", "character varying")
                               }

        global_all_attribs = [['c_custkey', 'c_name', 'c_address', 'c_nationkey',
                               'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
                              ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                               'l_discount',
                               'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                               'l_shipinstruct',
                               'l_shipmode', 'l_comment'],
                              ["o_orderkey",
                               "o_custkey",
                               "o_orderstatus",
                               "o_totalprice",
                               "o_orderdate",
                               "o_orderpriority",
                               "o_clerk",
                               "o_shippriority",
                               "o_comment"]
                              ]

        filter_predicates = [('customer1', 'c_mktsegment', 'equal', 'BUILDING', 'BUILDING'),
                             ('orders1', 'o_orderdate', '<=', datetime.date(1, 1, 1), datetime.date(1995, 3, 14)),
                             ('lineitem1', 'l_shipdate', '>=', datetime.date(1995, 3, 16), datetime.date(9999, 12, 31))]

        join_graph = [['c_custkey', 'o_custkey'], ['o_orderkey', 'l_orderkey']]

        global_min_instance_dict = {}

        pj = Projection(self.conn, global_attrib_types, core_rels, filter_predicates, join_graph, global_all_attribs,
                        global_min_instance_dict, aoa_predicates)
        pj.mock = True

        self.conn.execute_sql(["drop table if exists customer1;", "create table customer1 (like customer);",
                               f"Insert into customer1(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
                               f"c_mktsegment,c_comment)"
                               f"VALUES (136777,\'Customer#000060217\',\'kolkata\',2,\'27-299-23-31\',4089.02,"
                               f"\'BUILDING\',\'Nothing\');",

                               "drop table if exists lineitem1;", "create table lineitem1 (like lineitem);",
                               f"Insert into lineitem1(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
                               f"l_extendedprice,l_discount,"
                               f"l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,"
                               f"l_shipmode,l_comment) "
                               f"VALUES (136777,136777,136777,2,25.0,4089.02,10.0,12.02,\'A\',\'F\',\'1997-01-01\',"
                               f"\'1995-01-01\',\'1995-01-01\',\'COD COLLECT\',\'AIR\',\'Nothing\');",

                               "drop table if exists orders1;", "create table orders1 (like orders);",
                               f"Insert into orders1(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
                               f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
                               f"VALUES (136777,136777,\'N\',9991.32,\'1993-01-01\', \'URGENT\', \'clerk#0000001\',"
                               f"0,\'hello world bye bye\');",
                               ])

        pj.do_init()
        s_values = []
        attribs, names, deps, flag = pj.find_projection_dependencies(q, s_values)
        print("===========")
        print(attribs)
        print(deps)
        print("===========")

        self.assertEqual(len(attribs), 4)
        self.assertTrue('l_orderkey' in attribs)
        self.assertTrue('' in attribs)
        self.assertTrue('o_orderdate' in attribs)
        self.assertTrue('o_shippriority' in attribs)

        self.assertEqual(len(deps), 4)
        self.assertTrue([('lineitem1', 'l_orderkey')] in deps)
        self.assertTrue([('orders1', 'o_orderdate')] in deps)
        self.assertTrue([('orders1', 'o_shippriority')] in deps)
        i = 0
        for dep in deps:
            if len(dep) > 1:
                self.assertTrue(('lineitem1', 'l_extendedprice') in dep)
                self.assertTrue(('lineitem1', 'l_discount') in dep)
                break
            i += 1
        self.assertEqual(names[i],'revenue')

        self.conn.execute_sql(["drop table if exists customer1;",
                               "drop table if exists lineitem1;", "drop table if exists orders1;"])

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
