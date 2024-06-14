import datetime
import unittest
import sys
from dataclasses import dataclass
from decimal import Decimal

from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper
from mysite.unmasque.src.core.dataclass.genPipeline_context import GenPipelineContext
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory

sys.path.append("../../../")
from mysite.unmasque.src.core.groupby_clause import GroupBy
from mysite.unmasque.test.util import tpchSettings, queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


@dataclass
class MockPGAOCtx:
    projected_attribs: list


@dataclass
class MockFilter:
    global_all_attribs: dict
    global_attrib_types: list
    global_min_instance_dict: dict
    attrib_types_dict: dict

    def get_dmin_val(self, attrib: str, tab: str):
        values = self.global_min_instance_dict[tab]
        attribs, vals = values[0], values[1]
        attrib_idx = attribs.index(attrib)
        val = vals[attrib_idx]
        ret_val = float(val) if isinstance(val, Decimal) else val
        return ret_val

    def get_datatype(self, tab_attrib):
        if any(x in self.attrib_types_dict[tab_attrib] for x in ['int', 'integer', 'number']):
            return 'int'
        elif 'date' in self.attrib_types_dict[tab_attrib]:
            return 'date'
        elif any(x in self.attrib_types_dict[tab_attrib] for x in ['text', 'char', 'varbit']):
            return 'str'
        elif any(x in self.attrib_types_dict[tab_attrib] for x in ['numeric', 'float']):
            return 'numeric'
        else:
            raise ValueError


@dataclass
class MockInequality:
    arithmetic_filters: list
    algebraic_eq_predicates: list
    aoa_predicates: list
    aoa_less_thans: list
    global_min_instance_dict: dict
    core_relations: list
    connectionHelper: AbstractConnectionHelper

    def do_permanent_mutation(self):
        pass

    def restore_d_min_from_dict(self):
        if not len(self.global_min_instance_dict):
            return
        for tab in self.core_relations:
            self.insert_into_dmin_dict_values(tab)

    def insert_into_dmin_dict_values(self, tabname):
        values = self.global_min_instance_dict[tabname]
        attribs, vals = values[0], values[1]
        attrib_list = ", ".join(attribs)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.truncate_table(tabname)])
        self.connectionHelper.execute_sql_with_params(
            self.connectionHelper.queries.insert_into_tab_attribs_format(f"({attrib_list})", "", tabname), [vals])


class MockGenPipelineCtx(GenPipelineContext):
    def __init__(self, core_relations, aoa_extractor, filter_extractor,
                 global_min_instance_dict: dict, or_predicates):
        super().__init__(core_relations, aoa_extractor, filter_extractor, global_min_instance_dict, or_predicates)


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelperFactory().createConnectionHelper()

    def see_d_min(self, tabs):
        print("======================")
        for tab in tabs:
            res, des = self.conn.execute_sql_fetchall(self.conn.queries.get_star(tab))
            print(f"-----  {tab} ------")
            print(res)
        print("======================")

    def do_setUp(self, tables):
        self.conn.connectUsingParams()
        for rel in tables:
            self.conn.execute_sql(["BEGIN;", f"drop table if exists {rel}_backup;",
                                   f"alter table {rel} rename to {rel}_backup;",
                                   f"create table {rel} (like {rel}_backup);", "COMMIT;"])
        self.conn.closeConnection()

    def do_tearDown(self, tables):
        self.conn.connectUsingParams()
        for rel in tables:
            self.conn.execute_sql(["BEGIN;",
                                   f"drop table {rel};",
                                   f"alter table {rel}_backup rename to {rel};", "COMMIT;"])
        self.conn.closeConnection()

    def test_sumang_Q6(self):
        from_rels = ['nation', 'partsupp', 'supplier']
        self.do_setUp(from_rels)
        self.__sumang_thesis_Q6(from_rels)
        self.do_tearDown(from_rels)

    def __sumang_thesis_Q6(self, from_rels):
        global_min_instance_dict = {
            'partsupp': [('ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'),
                         (84936, 4937, 8444, 26.97,
                          'riously final instructions. pinto beans cajole. idly even packages haggle doggedly '
                          'furiously regular ')],
            'nation': [('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'), (21, 'IRAN', 3, 'just a comment')],
            'supplier': [('s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'),
                         (4937, 'Supplier#0000000123',
                          'Kolkata',
                          21, '03242-256500', 487.02, 'Just another comment')]}
        filter_extractor = MockFilter(global_all_attribs={
            'nation': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'],
            'partsupp': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
            'supplier': ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
        },
            global_attrib_types=[('nation', 'n_nationkey', 'integer'),
                                 ('nation', 'n_name', 'character'),
                                 ('nation', 'n_regionkey', 'integer'),
                                 ('nation', 'n_comment', 'character varying'),
                                 ('partsupp', 'ps_partkey', 'bigint'),
                                 ('partsupp', 'ps_suppkey', 'bigint'),
                                 ('partsupp', 'ps_availqty', 'integer'),
                                 ('partsupp', 'ps_supplycost', 'numeric'),
                                 ('partsupp', 'ps_comment', 'character varying'),
                                 ('supplier', 's_suppkey', 'integer'),
                                 ('supplier', 's_name', 'character'),
                                 ('supplier', 's_address', 'character varying'),
                                 ('supplier', 's_nationkey', 'integer'),
                                 ('supplier', 's_phone', 'character'),
                                 ('supplier', 's_acctbal', 'numeric'),
                                 ('supplier', 's_comment', 'character varying')
                                 ],
            global_min_instance_dict=global_min_instance_dict,
            attrib_types_dict={}
        )
        eq_joins = [[('nation', 'n_nationkey'), ('supplier', 's_nationkey')],
                    [('supplier', 's_suppkey'), ('partsupp', 'ps_suppkey')]]

        aoa_extractor = MockInequality(arithmetic_filters=[],
                                       algebraic_eq_predicates=eq_joins,
                                       aoa_predicates=[],
                                       aoa_less_thans=[],
                                       global_min_instance_dict=global_min_instance_dict,
                                       core_relations=from_rels,
                                       connectionHelper=self.conn)

        or_predicates = [
            [('nation', 'n_name', 'equal', 'ARGENTINA', 'ARGENTINA'), ('nation', 'n_regionkey', '=', '3', '3')],
            [('nation', 'n_nationkey', '<=', -2147483648, 19), ('partsupp', 'ps_supplycost', '<=', -2147483648.88, 800),
             ('supplier', 's_acctbal', '>=', 500, 2147483647.88)]
        ]
        query = "select n_name, s_name, SUM(s_acctbal) from supplier, nation, partsupp where ps_suppkey=s_suppkey AND" \
                " (ps_supplycost < 800 or s_acctbal > 500 or n_nationkey < 20) and s_nationkey=n_nationkey " \
                "and (n_name = 'ARGENTINA' or n_regionkey =3) " \
                "group by n_name, s_name ORDER " \
                "BY n_name, s_name;"
        gen_ctx = GenPipelineContext(from_rels, aoa_extractor, filter_extractor, global_min_instance_dict,
                                     or_predicates)
        self.conn.connectUsingParams()
        gen_ctx.doJob()
        filter_extractor.attrib_types_dict = gen_ctx.attrib_types_dict
        self.see_d_min(from_rels)
        self.conn.closeConnection()

        pgao_ctx = MockPGAOCtx(projected_attribs=['n_name', 's_name', ''])
        gb = GroupBy(self.conn, gen_ctx, pgao_ctx)
        self.conn.connectUsingParams()
        check = gb.doJob(query)
        self.assertTrue(check)
        self.assertTrue(gb.has_groupby)
        self.assertEqual(len(gb.group_by_attrib), 2)
        self.assertTrue('n_name' in gb.group_by_attrib)
        self.assertTrue('s_name' in gb.group_by_attrib)
        self.conn.closeConnection()

    """
    def test_gb_Q1(self):
        global_min_instance_dict = {}
        self.conn.connectUsingParams()
        from_rels = tpchSettings.from_rels['Q1']
        global_key_attributes = ['l_orderkey', 'l_partkey', 'l_suppkey']

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
        gb = GroupBy(self.conn, delivery, pgao_ctx)
        gb.mock = True

        check = gb.doJob(queries.Q1)
        self.assertTrue(check)

        self.assertTrue(gb.has_groupby)
        self.assertEqual(2, len(gb.group_by_attrib))
        self.assertTrue('l_returnflag' in gb.group_by_attrib)
        self.assertTrue('l_linestatus' in gb.group_by_attrib)

        self.conn.closeConnection()

    def test_gb_Q3(self):
        global_min_instance_dict = {}
        global_key_attribs = ['c_custkey', 'c_nationkey', 'l_orderkey', 'l_partkey', 'l_suppkey',
                              'o_orderkey', 'o_custkey']

        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3_1']

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

        gb = GroupBy(self.conn, delivery, pgao_ctx)
        gb.mock = True

        check = gb.doJob(queries.Q3_1)
        self.assertTrue(check)

        self.assertTrue(gb.has_groupby)
        self.assertEqual(3, len(gb.group_by_attrib))
        self.assertTrue('l_orderkey' in gb.group_by_attrib)
        self.assertTrue('o_orderdate' in gb.group_by_attrib)
        self.assertTrue('o_shippriority' in gb.group_by_attrib)

        self.conn.closeConnection()

    def test_gb_Q4(self):
        global_min_instance_dict = {}
        global_key_attributes = ['o_orderkey', 'o_custkey']

        self.conn.connectUsingParams()
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

        projections = ['o_orderdate', 'o_orderpriority', '']

        gb = GroupBy(self.conn, delivery, pgao_ctx)
        gb.mock = True

        check = gb.doJob(queries.Q4)
        self.assertTrue(check)

        self.assertTrue(gb.has_groupby)
        self.assertEqual(2, len(gb.group_by_attrib))
        self.assertTrue('o_orderdate' in gb.group_by_attrib)
        self.assertTrue('o_orderpriority' in gb.group_by_attrib)

        self.conn.closeConnection()

    def test_gb_Q5(self):
        global_min_instance_dict = {}
        global_key_attribs = ['c_custkey', 'c_nationkey', 'l_orderkey', 'l_partkey', 'l_suppkey',
                              'o_orderkey', 'o_custkey', 's_suppkey', 's_nationkey', 'n_nationkey',
                              'n_regionkey', 'r_regionkey']

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

        projections = ['n_name', 'l_extendedprice']

        gb = GroupBy(self.conn, delivery, pgao_ctx)
        gb.mock = True

        check = gb.doJob(queries.Q5)
        self.assertTrue(check)

        self.assertTrue(gb.has_groupby)
        self.assertEqual(1, len(gb.group_by_attrib))
        self.assertTrue('n_name' in gb.group_by_attrib)

        self.conn.closeConnection()
    """


if __name__ == '__main__':
    unittest.main()
