import datetime
import unittest

import pytest as pytest

from ..refactored.nep import NEP
from ..src.core.QueryStringGenerator import QueryStringGenerator
from ..src.core.dataclass.generation_pipeline_package import PackageForGenPipeline
from ..src.util.constants import max_numeric_val
from ..test.util import tpchSettings
from ..test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def backup_tables(self):
        # self.conn.connectUsingParams()
        for tab in tpchSettings.relations:
            restore_name = self.conn.queries.get_backup(tab)
            self.conn.execute_sql([self.conn.queries.create_table_as_select_star_from(restore_name, tab)])
        # self.conn.closeConnection()

    def test_Q6_lineitem_returnflag(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem Where l_shipdate >= " \
                "'1994-01-01' and l_quantity < 24 " \
                "and l_returnflag <> 'R' Group By l_shipmode Limit 100;"

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate >= '1994-01-01' " \
              "and l_quantity <= 23.0 " \
              "Group By l_shipmode Limit 100; "

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1994, 1, 1), datetime.date(9999, 12, 31))]

        self.global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                                    ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                                    ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                                    ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
                                    ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                                    ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                                    ('lineitem', 'l_shipmode', 'character'),
                                    ('lineitem', 'l_comment', 'character varying')
                                    }

        self.global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        delivery = PackageForGenPipeline(['lineitem'],
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         [],
                                         [],
                                         {},
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()
        delivery.doJob()

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = 'l_shipdate >= \'1994-01-01\' and l_quantity <= 23.0 '

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True
        self.backup_tables()
        check = o.doJob([query, Q_E])
        self.assertTrue(check)
        print(o.Q_E)

        self.assertEqual("l_shipdate >= '1994-01-01' and l_quantity <= 23.0  and lineitem.l_returnflag <> 'R' ", q_gen.where_op)

        q_e = f"Select {q_gen.select_op}\n From {q_gen.from_op} \n Where {q_gen.where_op} \n" \
              f" Group By {q_gen.group_by_op} \n Limit {q_gen.limit_op};"
        self.assertEqual(q_e, o.Q_E)

        self.conn.closeConnection()

    def test_for_numeric_filter_NEP(self):
        self.conn.connectUsingParams()
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name not LIKE 'B%' limit 10;"
        Q_E = "select c_mktsegment as segment from customer,nation,orders where " \
              "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey;"
        core_rels = ['orders', 'nation','customer']
        filters = [('customer', 'c_acctbal', '<=', 1000, 5000)]

        aoa_predicates = []
        self.join_graph = [['c_nationkey', 'n_nationkey'],
                           ['c_custkey', 'o_custkey']]

        self.global_attrib_types = {
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
            ('nation', "n_name", "character"),
            ('nation', "n_regionkey", "integer"),
            ('nation', "n_comment", "character varying"),
            ('customer', 'c_custkey', 'integer'),
            ('customer', 'c_name', 'character varying'),
            ('customer', 'c_address', 'character varying'),
            ('customer', 'c_nationkey', 'integer'),
            ('customer', 'c_phone', 'character'),
            ('customer', 'c_acctbal', 'numeric'),
            ('customer', 'c_mktsegment', 'character'),
            ('customer', 'c_comment', 'character varying')
        }

        self.global_all_attribs = [
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
             "n_comment"],['c_custkey', 'c_name', 'c_address', 'c_nationkey',
                            'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'customer, orders, nation'
        q_gen.group_by_op = ''
        q_gen.limit_op = 10
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'c_mktsegment as segment'
        q_gen.where_op = "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey"

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         {},
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()
        delivery.doJob()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.set_all_relations(tpchSettings.relations)
        o.enabled = True
        o.mock = True
        self.backup_tables()

        check = o.doJob([query, Q_E])
        self.assertTrue(check)
        print(o.Q_E)
        self.assertEqual("c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey "
                         "and c_custkey = o_custkey and nation.n_name not LIKE 'B%' ", q_gen.where_op)
        q_e = f"Select {q_gen.select_op}\n From {q_gen.from_op} \n Where {q_gen.where_op} ;"
        self.assertEqual(q_e, o.Q_E)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_mukul_overlapping_ranges(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode, count(*) as count From lineitem Where l_quantity > 20 and l_quantity <> 25 " \
                "Group By l_shipmode Order By l_shipmode;"
        Q_E = "Select l_shipmode, count(*) as count From lineitem Where l_quantity > 20 " \
              "Group By l_shipmode Order By l_shipmode;"

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', 21.0, max_numeric_val)]

        self.global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                                    ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                                    ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                                    ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
                                    ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                                    ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                                    ('lineitem', 'l_shipmode', 'character'),
                                    ('lineitem', 'l_comment', 'character varying')
                                    }

        self.global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = None
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'l_shipmode'
        q_gen.select_op = 'l_shipmode, count(*) as count'
        q_gen.where_op = "l_quantity > 20 "

        self.global_min_instance_dict = {}
        aoa_predicates = []
        join_graph = []

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()
        delivery.doJob()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True
        self.backup_tables()

        check = o.doJob([query, Q_E])
        self.assertTrue(check)
        print(o.Q_E)
        self.assertEqual("l_quantity > 20  and lineitem.l_quantity <> 25.00", q_gen.where_op)

        self.conn.closeConnection()

    def test_two_neps_one_table(self):
        self.conn.connectUsingParams()

        global_min_instance_dict = {}

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

        self.global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                                    ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                                    ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                                    ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
                                    ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                                    ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                                    ('lineitem', 'l_shipmode', 'character'),
                                    ('lineitem', 'l_comment', 'character varying')
                                    }

        self.global_all_attribs = [
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

        aoa_predicates = []
        self.join_graph = []

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         {},
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()
        delivery.doJob()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True
        self.backup_tables()

        check = o.doJob([query, Q_E])
        self.assertTrue(check)
        print(o.Q_E)

        self.assertTrue("l_linenumber <> 4" in q_gen.where_op)
        self.assertTrue("l_returnflag <> 'R'" in q_gen.where_op)
        terms = o.Q_E.split(" ")
        and_count = terms.count("and")
        self.assertEqual(and_count, 3)

        self.conn.closeConnection()

    def test_Q6_mukul_thesis(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem Where l_quantity < 24 and l_shipdate <> " \
                "'1994-01-02' " \
                "and l_shipmode " \
                "not like '%AIR%' Group By l_shipmode Limit 100;"

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_quantity <= 23.0 " \
              "Group By l_shipmode Limit 100; "

        global_key_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey']

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0)]

        # ('lineitem', 'l_shipdate', '<=', datetime.date(1994, 1, 1), datetime.date(9999, 12, 31))]

        self.global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                                    ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                                    ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                                    ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
                                    ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                                    ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                                    ('lineitem', 'l_shipmode', 'character'),
                                    ('lineitem', 'l_comment', 'character varying')
                                    }

        self.global_all_attribs = [
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
        q_gen.where_op = 'l_quantity <= 23.0 '

        global_min_instance_dict = {}
        aoa_predicates = []
        self.join_graph = []

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         {},
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()

        delivery.doJob()
        self.backup_tables()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True

        check = o.doJob([query, Q_E])
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("and lineitem.l_shipmode NOT LIKE '%AIR%'" in q_gen.where_op)
        self.assertTrue("and lineitem.l_shipdate <> '1994-01-02'" in q_gen.where_op)
        terms = o.Q_E.split(" ")
        and_count = terms.count("and")
        self.assertEqual(and_count, 2)

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

        aoa_predicates = []
        self.join_graph = [['s_suppkey', 'l_suppkey'],
                           ['o_orderkey', 'l_orderkey'],
                           ['s_nationkey', 'n_nationkey']]

        self.global_attrib_types = {('supplier', "s_suppkey", "integer"),
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
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
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
                                    ('nation', "n_name", "character"),
                                    ('nation', "n_regionkey", "integer"),
                                    ('nation', "n_comment", "character varying")
                                    }

        self.global_all_attribs = [
            ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
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

        self.global_min_instance_dict = {}

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()
        self.backup_tables()

        delivery.doJob()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True

        check = o.doJob([q, eq])
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("and nation.n_name <> 'GERMANY" in q_gen.where_op)
        terms = o.Q_E.split(" ")
        and_count = terms.count("and")
        self.assertEqual(and_count, 4)

        self.conn.closeConnection()

    def test_mukul_thesis_Q18(self):
        self.conn.connectUsingParams()
        q = "Select c_name, o_orderdate, o_totalprice, sum(l_quantity) From customer, orders, lineitem " \
            "Where c_phone Like '27-_%' and c_custkey = o_custkey and o_orderkey = l_orderkey and " \
            "c_name <> 'Customer#000060217'" \
            "Group By c_name, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"

        eq = "Select c_name, o_orderdate, o_totalprice, sum(l_quantity) From customer, orders, lineitem " \
             "Where c_phone Like '27-_%' and c_custkey = o_custkey and o_orderkey = l_orderkey " \
             "Group By c_name, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"

        core_rels = ['customer', 'lineitem', 'orders']

        global_key_attribs = ['c_custkey', 'c_nationkey', 'l_orderkey', 'l_partkey', 'l_suppkey',
                              'o_orderkey', 'o_custkey']

        filters = [('customer', 'c_phone', 'LIKE', '27-_%', '27-_%')]

        self.global_attrib_types = {('customer', 'c_custkey', 'integer'),
                                    ('customer', 'c_name', 'character varying'),
                                    ('customer', 'c_address', 'character varying'),
                                    ('customer', 'c_nationkey', 'integer'),
                                    ('customer', 'c_phone', 'character'),
                                    ('customer', 'c_acctbal', 'numeric'),
                                    ('customer', 'c_mktsegment', 'character'),
                                    ('customer', 'c_comment', 'character varying'),
                                    ('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                                    ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                                    ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                                    ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
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
                                    ('orders', "o_comment", "character varying")
                                    }

        self.global_all_attribs = [['c_custkey', 'c_name', 'c_address', 'c_nationkey',
                                    'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
                                   ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                                    'l_extendedprice',
                                    'l_discount',
                                    'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                                    'l_receiptdate',
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

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'customer, lineitem, orders'
        q_gen.group_by_op = 'c_name, o_orderdate, o_totalprice'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'o_orderdate, o_totalprice desc'
        q_gen.select_op = 'c_name, o_orderdate, o_totalprice, sum(l_quantity)'
        q_gen.where_op = 'c_phone Like \'27-_%\' and c_custkey = o_custkey and o_orderkey = l_orderkey '

        self.global_min_instance_dict = {}
        aoa_predicates = []
        self.join_graph = [['c_custkey', 'o_custkey'],
                           ['o_orderkey', 'l_orderkey']]

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()

        delivery.doJob()
        self.backup_tables()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True

        check = o.doJob([q, eq])
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("customer.c_name <> 'Customer#000060217'" in q_gen.where_op)
        terms = o.Q_E.split(" ")
        and_count = terms.count("and")
        self.assertEqual(and_count, 3)

        self.conn.closeConnection()

    @pytest.mark.skip
    def test_mukul_thesis_Q18_modified(self):
        self.conn.connectUsingParams()
        q = "Select c_phone, o_orderdate, o_totalprice, sum(l_quantity) From customer, orders, lineitem " \
            "Where c_name LIKE 'Customer#%217' and c_custkey = o_custkey and o_orderkey = l_orderkey and " \
            "c_phone not Like '27-_%' " \
            "Group By c_phone, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"

        eq = "Select c_phone, o_orderdate, o_totalprice, sum(l_quantity) From customer, orders, lineitem " \
             "Where c_name = 'Customer#000060217' and c_custkey = o_custkey and o_orderkey = l_orderkey " \
             "Group By c_phone, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"

        core_rels = ['customer', 'lineitem', 'orders']

        global_key_attribs = ['c_custkey', 'c_nationkey', 'l_orderkey', 'l_partkey', 'l_suppkey',
                              'o_orderkey', 'o_custkey']

        filters = [('customer', 'c_name', 'LIKE', 'Customer#%217', 'Customer#%217')]

        self.global_attrib_types = {('customer', 'c_custkey', 'integer'),
                                    ('customer', 'c_name', 'character varying'),
                                    ('customer', 'c_address', 'character varying'),
                                    ('customer', 'c_nationkey', 'integer'),
                                    ('customer', 'c_phone', 'character'),
                                    ('customer', 'c_acctbal', 'numeric'),
                                    ('customer', 'c_mktsegment', 'character'),
                                    ('customer', 'c_comment', 'character varying'),
                                    ('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                                    ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                                    ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                                    ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                                    ('lineitem', 'l_returnflag', 'character'),
                                    ('lineitem', 'l_linestatus', 'character'),
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
                                    ('orders', "o_comment", "character varying")
                                    }

        self.global_all_attribs = [['c_custkey', 'c_name', 'c_address', 'c_nationkey',
                                    'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
                                   ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                                    'l_extendedprice',
                                    'l_discount',
                                    'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                                    'l_receiptdate',
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

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'customer, lineitem, orders'
        q_gen.group_by_op = 'c_phone, o_orderdate, o_totalprice'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'o_orderdate, o_totalprice desc'
        q_gen.select_op = 'c_phone, o_orderdate, o_totalprice, sum(l_quantity)'
        q_gen.where_op = 'c_name LIKE \'Customer#%217\' and c_custkey = o_custkey and o_orderkey = l_orderkey '

        self.global_min_instance_dict = {}
        aoa_predicates = []
        self.join_graph = [['c_custkey', 'o_custkey'],
                           ['o_orderkey', 'l_orderkey']]
        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()

        delivery.doJob()

        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True

        check = o.doJob([q, eq])
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("c_phone NOT LIKE '27-%'" in q_gen.where_op)
        terms = o.Q_E.split(" ")
        and_count = terms.count("and")
        self.assertEqual(and_count, 3)

        self.conn.closeConnection()

    def test_mukul_thesis_Q18_modified_fix_try(self):
        rep_str = '27-922-412-7917'

        self.conn.connectUsingParams()
        q = "Select c_phone, o_orderdate, o_totalprice, sum(l_quantity) From customer1, orders, lineitem " \
            "Where c_name LIKE 'Customer#%7' and c_custkey = o_custkey and o_orderkey = l_orderkey and " \
            "c_phone not Like '27-_%' " \
            "Group By c_phone, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"

        self.conn.execute_sql(["drop table if exists customer1;", "create table customer1 (like customer);",
                               f"Insert into customer1(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
                               f"c_mktsegment,c_comment)"
                               f"VALUES (136777,\'Customer#000060217\',\'kolkata\',2,\'{rep_str}\',4089.02,"
                               f"\'BUILDING\',\'Nothing\');"])

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'customer1, lineitem, orders'
        q_gen.group_by_op = 'c_phone, o_orderdate, o_totalprice'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'o_orderdate, o_totalprice desc'
        q_gen.select_op = 'c_phone, o_orderdate, o_totalprice, sum(l_quantity)'
        q_gen.where_op = 'c_name LIKE \'Customer#%7\' and c_custkey = o_custkey and o_orderkey = l_orderkey'

        rep_str_wildcard = q_gen.getStrFilterValue(q, "customer1", "c_phone", rep_str, 500)
        self.assertEqual(rep_str_wildcard, "27-%")
        self.conn.execute_sql(["drop table customer1;"])
        self.conn.closeConnection()

    def test_mukul_thesis_Q11(self):
        self.conn.connectUsingParams()

        q = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation " \
            "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' " \
            "and ps_COMMENT not like '%regular%dependencies%' and s_acctbal <> 2177.90 " \
            "Group By ps_COMMENT " \
            "Order by value desc Limit 100;"

        eq = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation " \
             "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT " \
             "Order by value desc Limit 100;"

        core_rels = ['partsupp', 'supplier', 'nation']

        global_key_attribs = ['ps_partkey', 'ps_suppkey', 's_suppkey', 's_nationkey',
                              'n_nationkey', 'n_regionkey']

        filters = [('nation', 'n_name', '=', 'ARGENTINA', 'ARGENTINA')]

        self.global_attrib_types = {('partsupp', 'ps_partkey', 'integer'),
                                    ('partsupp', 'ps_suppkey', 'integer'),
                                    ('partsupp', 'ps_availqty', 'integer'),
                                    ('partsupp', 'ps_supplycost', 'numeric'),
                                    ('partsupp', 'ps_comment', 'character varying'),
                                    ('supplier', "s_suppkey", "integer"),
                                    ('supplier', "s_name", "character"),
                                    ('supplier', "s_address", "character varying"),
                                    ('supplier', "s_nationkey", "integer"),
                                    ('supplier', "s_phone", "character"),
                                    ('supplier', "s_acctbal", "numeric"),
                                    ('supplier', "s_comment", "character varying"),
                                    ('nation', "n_nationkey", "integer"),
                                    ('nation', "n_name", "character"),
                                    ('nation', "n_regionkey", "integer"),
                                    ('nation', "n_comment", "character varying")
                                    }

        self.global_all_attribs = [['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
                                   ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal",
                                    "s_comment"],
                                   ["n_nationkey",
                                    "n_name",
                                    "n_regionkey",
                                    "n_comment"]]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'partsupp, supplier, nation'
        q_gen.group_by_op = 'ps_COMMENT'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'value desc'
        q_gen.select_op = 'ps_COMMENT, sum(ps_supplycost * ps_availqty) as value'
        q_gen.where_op = 'ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = \'ARGENTINA\''

        self.global_min_instance_dict = {}
        aoa_predicates = []
        self.join_graph = [['ps_suppkey', 's_suppkey'],
                           ['s_nationkey', 'n_nationkey']]

        delivery = PackageForGenPipeline(core_rels,
                                         self.global_all_attribs,
                                         self.global_attrib_types,
                                         filters,
                                         [],
                                         self.join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)
        self.do_init()
        delivery.doJob()
        o = NEP(self.conn, core_rels, tpchSettings.all_size, q_gen, delivery)
        o.enabled = True
        o.mock = True
        self.backup_tables()

        check = o.doJob([q, eq])
        print(q_gen.where_op)
        print(o.Q_E)

        self.assertTrue(check)
        self.assertTrue("ps_comment NOT LIKE '%regular%dependencies%'" in q_gen.where_op)
        self.assertTrue("s_acctbal <> 2177.90" in q_gen.where_op)
        terms = o.Q_E.split(" ")
        and_count = terms.count("and")
        self.assertEqual(and_count, 4)

        self.conn.closeConnection()

    def test_Q11_q_gen_fix_try(self):
        rep_str = "ide of the carefully regular accounts. " \
                  "final dependencies x-ray according to the pending, " \
                  "unusual asymptotes. final foxes wake against the special requests. regular gr"

        self.conn.connectUsingParams()
        self.conn.execute_sql(["drop table if exists partsupp1;", "create table partsupp1 (like partsupp);",
                               f"Insert into partsupp1(ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment) "
                               f"VALUES (152257,4773,2,2.00,\'{rep_str}\');"])

        q = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp1, supplier, nation " \
            "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' " \
            "and ps_COMMENT not like '%regular%dependencies%' " \
            "Group By ps_COMMENT " \
            "Order by value desc Limit 100;"

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'partsupp1, supplier, nation'
        q_gen.group_by_op = 'ps_COMMENT'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'value desc'
        q_gen.select_op = 'ps_COMMENT, sum(ps_supplycost * ps_availqty) as value'
        q_gen.where_op = 'ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = \'ARGENTINA\''

        rep_str_wildcard = q_gen.getStrFilterValue(q, "partsupp1", "ps_comment", rep_str, 500)
        self.assertEqual(rep_str_wildcard, "%regular%dependencies%")
        self.conn.execute_sql(["drop table partsupp1;"])
        self.conn.closeConnection()

    def test_remove_NE_string_q_gen(self):
        elf = ['partsupp', 'ps_comment', '<>', 'hello world regular mina dependencies']
        q_gen = QueryStringGenerator(self.conn)
        q_gen.where_op = 'ps_suppkey = s_suppkey and s_nationkey = n_nationkey and ' \
                         'n_name = \'ARGENTINA\' and ps_comment <> \'dependencies\' ' \
                         'and ps_comment <> \'hello world regular mina dependencies\''
        q_gen.remove_exact_NE_string_predicate(elf)
        print(q_gen.where_op)
        self.assertEqual(q_gen.where_op, 'ps_suppkey = s_suppkey and '
                                         's_nationkey = n_nationkey and n_name = \'ARGENTINA\'')


if __name__ == '__main__':
    unittest.main()
