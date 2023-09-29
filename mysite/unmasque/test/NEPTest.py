import datetime
import unittest

from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.refactored.nep import NEP
from mysite.unmasque.src.core.QueryStringGenerator import QueryStringGenerator
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_Q6_mukul_thesis_shipdate(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem Where l_shipdate >= " \
                "'1994-01-01' and l_quantity < 24 " \
                "and l_shipdate <> '1994-07-11' Group By l_shipmode Limit 100;"

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate >= '1994-01-01' " \
              "and l_quantity <= 23.0 " \
              "Group By l_shipmode Limit 100; "

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1994, 1, 1), datetime.date(9999, 12, 31))]

        global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')
                               }

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = 'l_shipdate >= \'1994-01-01\' and l_quantity <= 23.0 '

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                global_key_attribs,
                q_gen)

        check = o.doJob(query, Q_E)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("and l_shipdate <> '1994-07-11'" in o.Q_E)

        self.conn.closeConnection()

    def test_mukul_sample_query(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode, count(*) as count From lineitem Where l_quantity > 20 and l_quantity <> 25 " \
                "Group By l_shipmode Order By l_shipmode;"
        Q_E = "Select l_shipmode, count(*) as count From lineitem Where l_quantity > 20 " \
              "Group By l_shipmode Order By l_shipmode;"

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 20.0)]

        global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')
                               }

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = ''
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'l_shipmode'
        q_gen.select_op = 'l_shipmode, count(*) as count'
        q_gen.where_op = "l_quantity > 20 "

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                global_key_attribs,
                q_gen)

        check = o.doJob(query, Q_E)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue(" Where l_quantity > 20 and l_quantity <> 25 " in o.Q_E)

        self.conn.closeConnection()

    def test_something(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem " \
                "Where l_shipdate  < '1994-01-01' " \
                "and l_quantity < 24 " \
                "and l_linenumber <> 4 and l_returnflag <> 'R' " \
                "Group By l_shipmode Limit 100; "

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate <= '1993-12-31' " \
              "and l_quantity < 24 " \
              "Group By l_shipmode Limit 100; "

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1, 1, 1), datetime.date(1993, 12, 31))]

        global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')
                               }

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = "l_quantity  <= 23.0 and l_shipdate  <= '1993-12-31'"

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                global_key_attribs,
                q_gen)

        check = o.doJob(query, Q_E)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("Where l_quantity  <= 23.0 and l_shipdate  <= '1993-12-31' and l_linenumber <> 4" in o.Q_E)

        self.conn.closeConnection()

    def test_Q6_mukul_thesis(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem Where l_shipdate >= " \
                "'1994-01-01' and l_quantity < 24 " \
                "and l_shipmode " \
                "not like '%AIR%' and l_shipdate <> '1995-01-03' Group By l_shipmode Limit 100;"

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate >= '1994-01-01' " \
              "and l_quantity <= 23.0 " \
              "Group By l_shipmode Limit 100; "

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1994, 1, 1), datetime.date(9999, 12, 31))]

        global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')
                               }

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = 'l_shipdate >= \'1994-01-01\' and l_quantity <= 23.0 '

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                global_key_attribs,
                q_gen)

        check = o.doJob(query, Q_E)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("and l_shipmode NOT LIKE '%AIR%'" in o.Q_E)
        self.assertTrue("and l_shipdate <> '1994-01-03'" in o.Q_E)

        self.conn.closeConnection()

    def test_Q21_mukul_thesis(self):
        self.conn.connectUsingParams()
        q = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation " \
            "Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' " \
            "and s_nationkey = n_nationkey and n_name <> 'GERMANY' Group By s_name " \
            "Order By numwait desc, s_name Limit 100;"
        eq = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation " \
             "Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' " \
             "and s_nationkey = n_nationkey Group By s_name " \
             "Order By numwait desc, s_name Limit 100;"

        core_rels = ['supplier', 'lineitem', 'orders', 'nation']

        global_key_attribs = ['s_suppkey', 's_nationkey', 'l_orderkey', 'l_partkey', 'l_suppkey',
                              'o_orderkey', 'o_custkey', 'n_nationkey', 'n_regionkey']

        filters = [('orders', 'o_orderstatus', 'equal', 'F', 'F')]

        global_attrib_types = {('supplier', "s_suppkey", "integer"),
                               ('supplier', "s_name", "character"),
                               ('supplier', "s_address", "character varying"),
                               ('supplier', "s_nationkey", "integer"),
                               ('supplier', "s_phone", "character"),
                               ('supplier', "s_acctbal", "numeric"),
                               ('supplier', "s_comment", "character varying"),
                               ('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying'),
                               ('orders', "o_orderkey", "integer"),
                               ('orders', "o_custkey", "integer"),
                               ('orders', "o_orderstatus", "character"),
                               ('orders', "o_totalprice", "numeric"),
                               ('orders', "o_orderdate", "date"),
                               ('orders', "o_orderpriority", "character"),
                               ('orders', "o_clerk", "character"),
                               ('orders', "o_shippriority", "integer"),
                               ('orders', "o_comment", "character varying"),
                               ('nation', "n_nationkey", "integer"),
                               ('orders', "n_name", "character"),
                               ('orders', "n_regionkey", "integer"),
                               ('orders', "n_comment", "character varying"),
                               ('nation', "n_nationkey", "integer"),
                               ('nation', "n_name", "character"),
                               ('nation', "n_regionkey", "integer"),
                               ('nation', "n_comment", "character varying")
                               }

        global_all_attribs = [["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
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
                               "o_comment"],
                              ["n_nationkey",
                               "n_name",
                               "n_regionkey",
                               "n_comment"]]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'supplier, lineitem, orders, nation'
        q_gen.group_by_op = 's_name'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'numwait desc, s_name'
        q_gen.select_op = 's_name, count(*) as numwait'
        q_gen.where_op = 's_suppkey = l_suppkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey  and ' \
                         'o_orderstatus = \'F\''

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                global_key_attribs,
                q_gen)

        check = o.doJob(q, eq)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("and n_name <> 'GERMANY'" in o.Q_E)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
