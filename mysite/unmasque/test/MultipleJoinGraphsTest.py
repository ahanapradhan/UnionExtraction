import datetime
import unittest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.core.multiple_equi_joins import MultipleEquiJoin
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    tab_customer = "customer"
    tab_nation = "nation"
    tab_supplier = "supplier"
    tab_orders = "orders"
    tab_lineitem = "lineitem"

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

    def test_adonis_case_2(self):
        relations = [self.tab_orders, self.tab_customer, self.tab_lineitem]
        self.conn.connectUsingParams()

        self.conn.execute_sql([
            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (126908, \'Customer#000126908\', \'FNX3ShxqhJHfjbvvDWFgv0\', "
            f"10, \'20-783-624-7251\', 2673.27, \'BUILDING  \',"
            f" \'ccounts. quickly regular ideas cajole quickly. quickly final "
            f"requests after the fluffily ironic deposits are\');",

            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,l_discount,"
            f"l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,"
            f"l_shipmode,l_comment) "
            f"VALUES (20581, 77669, 3935, 1, "
            f"2.00, 3293.32, 0.04, 0.00, \'N\', "
            f"\'O\', \'1996-11-25\', \'1996-11-27\', "
            f"\'1996-12-17\', "
            f"\'COLLECT COD              \', \'FOB       \', \'refully after the sly\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (20581, 106876, \'O\', 2673.27, \'1996-10-2\', "
            f"\'3-MEDIUM       \', \'Clerk#000000476\', 0, "
            f"\'totes sleep quickly along the slyly unusual foxes-- blithely silent pack\');",
            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (20928, 126908, \'P\', 114593.25, \'1995-4-17\', "
            f"\'2-HIGH         \', \'Clerk#000000091\', 0, \'ironic platelets x-ray furiou\');"
        ])

        query = "SELECT c_acctbal as price FROM customer, orders WHERE c_custkey = o_custkey " \
                "AND o_orderdate > DATE '1995-01-01' INTERSECT " \
                "SELECT o_totalprice as price FROM orders, lineitem WHERE o_orderkey = l_orderkey " \
                "AND l_shipdate > DATE '1995-01-01';"

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (20581, 106876, 'O', 2673.27, datetime.date(1996, 10, 2), '3-MEDIUM       ', 'Clerk#000000476', 0,
             'totes sleep quickly along the slyly unusual foxes-- blithely silent '
             'pack'),
            (20928, 126908, 'P', 114593.25, datetime.date(1995, 4, 17),
             '2-HIGH         ', 'Clerk#000000091', 0,
             'ironic platelets x-ray furiou')], 'customer': [
            ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),
            (126908, 'Customer#000126908', 'FNX3ShxqhJHfjbvvDWFgv0', 10, '20-783-624-7251', 2673.27, 'BUILDING  ',
             'ccounts. quickly regular ideas cajole quickly. quickly final requests after the fluffily ironic '
             'deposits are')], 'lineitem': [
            ('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
             'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
             'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
            (20581, 77669, 3935, 1, 2.0, 3293.32, 0.04, 0.0, 'N', 'O', datetime.date(1996, 11, 25),
             datetime.date(1996, 11, 27), datetime.date(1996, 12, 17), 'COLLECT COD              ', 'FOB       ',
             'refully after the sly')]}

        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))

        equi_join = MultipleEquiJoin(self.conn, tpchSettings.key_lists, relations, global_min_instance_dict)
        equi_join.mock = True
        check = equi_join.doJob(query)
        self.assertTrue(check)
        print(equi_join.global_join_graph)
        self.assertEqual(1, equi_join.join_key_subquery_dict['l_orderkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['o_orderkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['c_custkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['o_custkey'])

        print(equi_join.global_all_join_graphs)
        self.assertFalse(equi_join.validate_global_all_join_graphs(query))
        equi_join.global_all_join_graphs = [[['c_custkey', 'o_custkey']], [['o_orderkey', 'l_orderkey']]]
        self.assertTrue(equi_join.validate_global_all_join_graphs(query))

        # for join_graph in equi_join.global_all_join_graphs:
        #    self.assertEqual(len(join_graph), 1)

        global_graph = frozenset({frozenset({'c_custkey', 'o_custkey'}),
                                  frozenset({'l_orderkey', 'o_orderkey'})})

        self.conn.closeConnection()

    def test_adonis_case_2_true(self):
        relations = [self.tab_orders, self.tab_customer, self.tab_lineitem]
        self.conn.connectUsingParams()

        self.conn.execute_sql([
            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (126908, \'Customer#000126908\', \'FNX3ShxqhJHfjbvvDWFgv0\', "
            f"10, \'20-783-624-7251\', 2673.27, \'BUILDING  \',"
            f" \'ccounts. quickly regular ideas cajole quickly. quickly final "
            f"requests after the fluffily ironic deposits are\');",

            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,l_discount,"
            f"l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,"
            f"l_shipmode,l_comment) "
            f"VALUES (20581, 77669, 3935, 1, "
            f"2.00, 3293.32, 0.04, 0.00, \'N\', "
            f"\'O\', \'1996-11-25\', \'1996-11-27\', "
            f"\'1996-12-17\', "
            f"\'COLLECT COD              \', \'FOB       \', \'refully after the sly\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (20581, 106876, \'O\', 2673.27, \'1996-10-2\', "
            f"\'3-MEDIUM       \', \'Clerk#000000476\', 0, "
            f"\'totes sleep quickly along the slyly unusual foxes-- blithely silent pack\');",
            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (20928, 126908, \'P\', 114593.25, \'1995-4-17\', "
            f"\'2-HIGH         \', \'Clerk#000000091\', 0, \'ironic platelets x-ray furiou\');"
        ])

        query = "SELECT c_acctbal as price FROM customer, orders WHERE c_custkey = o_custkey " \
                "AND o_orderdate > DATE '1995-01-01' INTERSECT " \
                "SELECT o_totalprice as price FROM orders, lineitem WHERE o_orderkey = l_orderkey " \
                "AND l_shipdate > DATE '1995-01-01';"

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (20581, 106876, 'O', 2673.27, datetime.date(1996, 10, 2), '3-MEDIUM       ', 'Clerk#000000476', 0,
             'totes sleep quickly along the slyly unusual foxes-- blithely silent '
             'pack'),
            (20928, 126908, 'P', 114593.25, datetime.date(1995, 4, 17),
             '2-HIGH         ', 'Clerk#000000091', 0,
             'ironic platelets x-ray furiou')], 'customer': [
            ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),
            (126908, 'Customer#000126908', 'FNX3ShxqhJHfjbvvDWFgv0', 10, '20-783-624-7251', 2673.27, 'BUILDING  ',
             'ccounts. quickly regular ideas cajole quickly. quickly final requests after the fluffily ironic '
             'deposits are')], 'lineitem': [
            ('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
             'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
             'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
            (20581, 77669, 3935, 1, 2.0, 3293.32, 0.04, 0.0, 'N', 'O', datetime.date(1996, 11, 25),
             datetime.date(1996, 11, 27), datetime.date(1996, 12, 17), 'COLLECT COD              ', 'FOB       ',
             'refully after the sly')]}

        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))

        equi_join = MultipleEquiJoin(self.conn, tpchSettings.key_lists, relations, global_min_instance_dict)
        equi_join.mock = True
        check = equi_join.doJob(query)
        self.assertTrue(check)
        print(equi_join.global_join_graph)
        self.assertEqual(1, equi_join.join_key_subquery_dict['l_orderkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['o_orderkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['c_custkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['o_custkey'])

        equi_join.global_all_join_graphs = [[['c_custkey', 'o_custkey']], [['o_orderkey', 'l_orderkey']]]
        self.assertTrue(equi_join.validate_global_all_join_graphs(query))

        # for join_graph in equi_join.global_all_join_graphs:
        #    self.assertEqual(len(join_graph), 1)

        global_graph = frozenset({frozenset({'c_custkey', 'o_custkey'}),
                                  frozenset({'l_orderkey', 'o_orderkey'})})

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
