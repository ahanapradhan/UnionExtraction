import ast
import datetime
import sys
import unittest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import get_unused_dummy_val, get_format, get_char
from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper
from mysite.unmasque.src.core.dataclass.generation_pipeline_package import PackageForGenPipeline
from mysite.unmasque.src.util import constants
from mysite.unmasque.src.util.constants import IDENTICAL_EXPR
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase

sys.path.append("../../../")
from mysite.unmasque.refactored.aggregation import Aggregation
from mysite.unmasque.test.util import tpchSettings, queries


def construct_values_used(global_filter_predicates, attrib_types_dict):
    vu = []
    # Identifying projected attributs with no filter
    for pred in global_filter_predicates:
        vu.append(pred[1])
        if 'char' in attrib_types_dict[(pred[0], pred[1])] or 'text' in attrib_types_dict[
            (pred[0], pred[1])]:
            vu.append(pred[3].replace('%', ''))
        else:
            vu.append(pred[3])
    return vu


def construct_values_for_attribs(conn: AbstractConnectionHelper, value_used, global_join_graph, core_relations,
                                 global_all_attribs,
                                 attrib_types_dict):
    for elt in global_join_graph:
        dummy_int = get_unused_dummy_val('int', value_used)
        for val in elt:
            value_used.append(val)
            value_used.append(dummy_int)
    for i in range(len(core_relations)):
        tabname = core_relations[i]
        attrib_list = global_all_attribs[i]
        insert_values = []
        att_order = '('
        for attrib in attrib_list:
            att_order = att_order + attrib + ","
            if attrib in value_used:
                if 'int' in attrib_types_dict[(tabname, attrib)] \
                        or \
                        'numeric' in attrib_types_dict[(tabname, attrib)]:
                    insert_values.append(value_used[value_used.index(attrib) + 1])
                elif 'date' in attrib_types_dict[(tabname, attrib)]:
                    date_val = value_used[value_used.index(attrib) + 1]
                    date_insert = get_format('date', date_val)
                    insert_values.append(ast.literal_eval(date_insert))
                else:
                    insert_values.append(str(value_used[value_used.index(attrib) + 1]))

            else:
                value_used.append(attrib)
                if 'int' in attrib_types_dict[(tabname, attrib)] \
                        or \
                        'numeric' in attrib_types_dict[(tabname, attrib)]:
                    dummy_int = get_unused_dummy_val('int', value_used)
                    insert_values.append(dummy_int)
                    value_used.append(dummy_int)
                elif 'date' in attrib_types_dict[(tabname, attrib)]:
                    dummy_date = get_unused_dummy_val('date', value_used)
                    val = ast.literal_eval(get_format('date', dummy_date))
                    insert_values.append(val)
                    value_used.append(val)
                elif 'boolean' in attrib_types_dict[(tabname, attrib)]:
                    insert_values.append(constants.dummy_boolean)
                    value_used.append(str(constants.dummy_boolean))
                elif 'bit varying' in attrib_types_dict[(tabname, attrib)]:
                    value_used.append(attrib)
                    insert_values.append(constants.dummy_varbit)
                    value_used.append(str(constants.dummy_varbit))
                else:
                    dummy_char = get_unused_dummy_val('char', value_used)
                    dummy = get_char(dummy_char)
                    insert_values.append(dummy)
                    value_used.append(dummy)

        insert_values = tuple(insert_values)
        conn.queries.insert_attrib_vals_into_table(att_order, attrib_list, [insert_values], tabname)

    value_used = [str(val) for val in value_used]
    return value_used


def truncate_core_relations(from_rels, conn):
    for rel in from_rels:
        conn.execute_sql([conn.queries.truncate_relation(rel)])


def create_dmin_for_test(from_rels, global_filter_predicates, attrib_types_dict, global_all_attribs,
                         global_join_graph, conn, app, global_min_instance_dict):
    truncate_core_relations(from_rels)
    val_used = construct_values_used(global_filter_predicates, attrib_types_dict)
    val_used = construct_values_for_attribs(conn, val_used, global_join_graph, from_rels, global_all_attribs,
                                            attrib_types_dict)
    for tab_name in from_rels:
        al = app.doJob("select * from " + tab_name)
        global_min_instance_dict[tab_name] = [al[0], al[1]]
    print(global_min_instance_dict)


class MyTestCase(BaseTestCase):
    def __init__(self):
        super().__init__()
        self.core_relations = None
        self.global_all_attribs = None
        self.global_attrib_types = None
        self.filter_predicates = None
        self.join_graph = None
        self.global_min_instance_dict = None
        self.global_attrib_types_dict = {}
        self.app = Executable(self.conn)

    def setUp(self):
        super().setUp()
        self.conn.connectUsingParams()
        for rel in tpchSettings.relations:
            self.conn.execute_sql(["BEGIN;", f"alter table {rel} rename to {rel}_copy;",
                                   f"create table {rel} (like {rel}_copy);", "COMMIT;"])
        self.conn.closeConnection()

    def tearDown(self):
        self.conn.connectUsingParams()
        for rel in tpchSettings.relations:
            self.conn.execute_sql(["BEGIN;",
                                   f"drop table {rel};",
                                   f"alter table {rel}_copy rename to {rel};", "COMMIT;"])
        self.conn.closeConnection()
        super().tearDown()

    def post_process_for_generation_pipeline(self):
        self.pipeline_delivery = PackageForGenPipeline(self.core_relations,
                                                       self.global_all_attribs,
                                                       self.global_attrib_types,
                                                       self.filter_predicates,
                                                       [],
                                                       self.join_graph,
                                                       [],
                                                       self.global_min_instance_dict,
                                                       self.get_dmin_val,
                                                       self.get_datatype)
        self.pipeline_delivery.doJob()

    def test_agg_Q1(self):
        self.conn.connectUsingParams()
        self.global_min_instance_dict = {}
        self.core_relations = tpchSettings.from_rels['Q1']

        self.global_attrib_types = [('lineitem', 'l_orderkey', 'integer'),
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

        self.global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        self.filter_predicates = [('lineitem', 'l_shipdate', '<=', datetime.date(1998, 9, 21), datetime.date(9999, 12, 31))]

        self.join_graph = []

        projections = ['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount'
            , 'l_tax', 'l_quantity', 'l_extendedprice', 'l_discount', '']

        group_by_attribs = ['l_returnflag', 'l_linestatus']
        has_groupBy = True
        dep = [[('identical_expr_nc', 'l_returnflag')], [('identical_expr_nc', 'l_linestatus')],
               [('identical_expr_nc', 'l_quantity')], [('identical_expr_nc', 'l_extendedprice')],
               [('identical_expr_nc', 'l_discount')],
               [('identical_expr_nc', 'l_tax')], [('identical_expr_nc', 'l_quantity')],
               [('identical_expr_nc', 'l_extendedprice')], [('identical_expr_nc', 'l_discount')], []]
        sol = [[], [], [], []]
        p_list = [[], [], [], []]

        global_key_attribs = []

        self.do_init()
        create_dmin_for_test(self.core_relations, self.filter_predicates, self.global_attrib_types_dict,
                             self.global_all_attribs, self.join_graph, self.conn, self.app, self.global_min_instance_dict)
        delivery = PackageForGenPipeline(self.core_relations, self.global_all_attribs,
                                         self.global_attrib_types,
                                         self.filter_predicates,
                                         [],
                                         self.join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)
        delivery.doJob()

        agg = Aggregation(self.conn, projections, has_groupBy, group_by_attribs, dep, sol, p_list, delivery)
        agg.mock = True

        check = agg.doJob(queries.Q1)
        self.assertTrue(check)
        print("Agg List", agg.global_aggregated_attributes)

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
        self.global_min_instance_dict = {}

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

        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey']
        has_groupBy = True

        self.do_init()
        delivery = PackageForGenPipeline(from_rels, global_all_attribs,
                                         global_attrib_types,
                                         filter_predicates,
                                         [],
                                         join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)

        group_by_attribs = ['l_orderkey', 'o_totalprice', 'o_shippriority']
        dep = [[('identical_expr_nc', 'o_orderkey')], [('identical_expr_nc', 'l_discount')],
               [('identical_expr_nc', 'o_orderdate')], [('identical_expr_nc', 'o_shippriority')]]
        sol = [[], [[1.], [1.], [0.], [0.], [-1.], [-0.], [-0.], [0.]], [], []]

        agg = Aggregation(self.conn, join_graph, projections, has_groupBy, group_by_attribs, dep, sol, delivery)
        agg.mock = True

        check = agg.doJob(queries.Q3)
        self.assertTrue(check)
        print("Agg List", agg.global_aggregated_attributes)

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

    def test_projections_Q3_1(self):
        self.global_min_instance_dict = {}

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
        projections = ['o_orderkey', 'l_extendedprice*(1 - l_discount) + l_quantity', 'o_orderdate',
                       'o_shippriority']
        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey']

        has_groupBy = True
        group_by_attribs = ['l_orderkey', 'o_orderdate', 'o_shippriority']
        dep = [[(IDENTICAL_EXPR, 'o_orderkey')],
               [('lineitem', 'l_quantity'), ('lineitem', 'l_extendedprice'), ('lineitem', 'l_discount')],
               [(IDENTICAL_EXPR, 'o_orderdate')], [(IDENTICAL_EXPR, 'o_shippriority')]]
        sol = [[], [[1.], [1.], [0.], [0.], [-1.], [-0.], [-0.], [0.]], [], []]
        p_list = [[], ['l_quantity', 'l_extendedprice', 'l_discount', 'l_quantity*l_extendedprice',
                       'l_extendedprice*l_discount', 'l_discount*l_quantity', 'l_quantity*l_extendedprice*l_discount'],
                  [], []]

        self.do_init()
        delivery = PackageForGenPipeline(from_rels, global_all_attribs,
                                         global_attrib_types,
                                         filter_predicates,
                                         [],
                                         join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)

        agg = Aggregation(self.conn, join_graph, projections, has_groupBy, group_by_attribs, dep, sol, delivery)
        agg.mock = True

        check = agg.doJob(queries.Q3_1)
        self.assertTrue(check)
        print("After Test, Result", agg.global_aggregated_attributes)

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

    def test_projections_Q3_2(self):

        self.global_min_instance_dict = {}

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
        projections = ['o_orderkey', 'l_extendedprice*(1 - l_discount) - o_totalprice', 'o_orderdate',
                       'o_shippriority']
        global_key_attribs = ['l_orderkey', 'c_custkey', 'o_custkey', 'o_orderkey', ]
        has_groupBy = True
        group_by_attribs = ['l_orderkey', 'o_orderdate', 'o_shippriority']
        dep = [[(IDENTICAL_EXPR, 'o_orderkey')],
               [('orders', 'o_totalprice'), ('lineitem', 'l_extendedprice'), ('lineitem', 'l_discount')],
               [(IDENTICAL_EXPR, 'o_orderdate')], [(IDENTICAL_EXPR, 'o_shippriority')]]
        sol = [[], [[-1.], [1.], [0.], [0.], [-1.], [-0.], [-0.], [0.]], [], []]
        p_list = [[], ['o_totalprice', 'l_extendedprice', 'l_discount', 'o_totalprice*l_extendedprice',
                       'l_extendedprice*l_discount', 'l_discount*o_totalprice',
                       'o_totalprice*l_extendedprice*l_discount'],
                  [], []]

        self.do_init()
        delivery = PackageForGenPipeline(from_rels, global_all_attribs,
                                         global_attrib_types,
                                         filter_predicates,
                                         [],
                                         join_graph,
                                         [],
                                         self.global_min_instance_dict,
                                         self.get_dmin_val,
                                         self.get_datatype)

        agg = Aggregation(self.conn, join_graph, projections, has_groupBy, group_by_attribs, dep, sol, delivery)
        agg.mock = True

        check = agg.doJob(queries.Q3_2)
        self.assertTrue(check)
        print("After Test, Result", agg.global_aggregated_attributes)

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
