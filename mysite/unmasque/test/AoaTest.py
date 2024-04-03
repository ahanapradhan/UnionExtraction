import datetime

import pytest

from mysite.unmasque.refactored.limit import Limit
from mysite.unmasque.refactored.orderby_clause import OrderBy
from mysite.unmasque.refactored.projection import Projection
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.core.aoa import AlgebraicPredicate
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.src.util.aoa_utils import find_all_chains, create_adjacency_map_from_aoa_predicates, \
    merge_equivalent_paritions
from mysite.unmasque.test.AoaTestFullPipeline import get_subquery1, get_subquery2
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    tab_customer = "customer"
    tab_nation = "nation"
    tab_supplier = "supplier"
    tab_orders = "orders"
    tab_lineitem = "lineitem"

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.global_min_instance_dict = None
        self.pipeline = ExtractionPipeLine(self.conn)

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

    def test_directed_paths(self):
        edge_set = [
            (('partsupp', 'ps_availqty'), ('orders', 'o_orderkey')),
            (('lineitem', 'l_linenumber'), ('orders', 'o_orderkey')),
            (('partsupp', 'ps_availqty'), 2999749),
            (8444, ('orders', 'o_orderkey'))]
        E, L, absorbed_UBs, absorbed_LBs = edge_set, [], {}, {}
        directed_paths = find_all_chains(create_adjacency_map_from_aoa_predicates(E))
        print(directed_paths)
        self.assertEqual(len(directed_paths), 2)

    def test_dormant_aoa(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode, count(*) as count From orders, lineitem " \
                "Where " \
                "o_orderkey = l_orderkey " \
                "and l_commitdate <= l_receiptdate " \
                "and l_shipdate <= l_commitdate " \
                "and l_receiptdate >= '1994-01-01' " \
                "and l_receiptdate <= '1995-01-01' " \
                "and l_extendedprice <= o_totalprice " \
                "and l_extendedprice <= 70000 " \
                "and o_totalprice >= 60000 " \
                "Group By l_shipmode " \
                "Order By l_shipmode;"
        '''
        query = "Select l_shipmode, count(*) as count From orders, lineitem " \
                "Where " \
                "o_orderkey = l_orderkey " \
                "and l_commitdate <= l_receiptdate " \
                "and l_shipdate <= l_commitdate " \
                "and l_receiptdate >= '1994-01-01' " \
                "and l_receiptdate <= '1995-01-01' " \
                "Group By l_shipmode " \
                "Order By l_shipmode;"
        '''
        core_rels = ['orders', 'lineitem']

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2999908,2997, 4248, 6, 19.00, 36099.81, 0.04, 0.08, \'R\', \'F\', \'1994, 7, 17\',"
            f"\'1994, 7, 18\', \'1994, 8, 16\',\'NONE                     \', \'AIR       \',"
            f"\'re. unusual frets after the sl\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2999908,23014, \'F\', 168982.73, \'1994, 4, 30\', \'5-LOW\', \'Clerk#000000061\', 0, "
            f"\'ost slyly around the blithely bold requests.\');"])

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999908, 23014, 'F', 168982.73, datetime.date(1994, 4, 30), '5-LOW', 'Clerk#000000061', 0,
             'ost slyly around the blithely bold requests.')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                          'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
                         (2999908, 2997, 4248, 6, 19.00, 36099.81, 0.04, 0.08, 'R', 'F', datetime.date(1994, 7, 17),
                          datetime.date(1994, 7, 18), datetime.date(1994, 8, 16),
                          'NONE                     ', 'AIR       ',
                          're. unusual frets after the sl')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertTrue("l_orderkey = o_orderkey" in aoa.where_clause)
        self.assertTrue("l_extendedprice <= o_totalprice" in aoa.where_clause)
        self.assertTrue("l_shipdate <= l_commitdate" in aoa.where_clause)
        self.assertTrue("l_commitdate <= l_receiptdate" in aoa.where_clause)
        self.assertTrue("l_receiptdate <= '1995-01-01'" in aoa.where_clause)
        self.assertTrue("'1994-01-01' <= l_receiptdate" in aoa.where_clause)

        self.assertEqual(aoa.where_clause.count("and"), 7)
        self.conn.closeConnection()

    def test_multiple_aoa(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem', 'partsupp']
        query = "Select l_quantity, l_shipinstruct From orders, lineitem, partsupp " \
                "Where ps_partkey = l_partkey " \
                "and ps_suppkey = l_suppkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate " \
                "and ps_availqty <= l_orderkey " \
                "and l_extendedprice <= 20000 " \
                "and o_totalprice <= 60000 " \
                "and ps_supplycost <= 500 " \
                "and l_linenumber = 1 " \
                "Order By l_orderkey Limit 10;"

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2999749,84936, 4937, 1, 1.00, 1920.93,0.01, 0.02, \'N\', \'O\', \'1998, 5, 6\',"
            f"\'1998, 5, 4\', \'1998, 5, 30\',\'COLLECT COD              \', \'AIR       \',"
            f"\'esias. furiously final foxes detec\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2999749, 122899, \'O\', 56924.25, \'1998, 2, 8\', \'5-LOW\', \'Clerk#000000555\', 0, "
            f"\'luffily. slyly regular deposits will snooze. furiously pending asymptot\');",

            f"Insert into partsupp(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) "
            f"VALUES (84936, 4937, 8444, 26.97, \'riously final instructions. pinto beans cajole. "
            f"idly even packages haggle doggedly furiously regular \');"
        ])

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999749, 122899, 'O', 56924.25, datetime.date(1998, 2, 8), '5-LOW', 'Clerk#000000555', 0,
             'luffily. slyly regular deposits will snooze. furiously pending asymptot')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                          'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
                         (2999749, 84936, 4937, 1, 1.00, 1920.93,
                          0.01, 0.02, 'N', 'O', datetime.date(1998, 5, 6),
                          datetime.date(1998, 5, 4), datetime.date(1998, 5, 30),
                          'COLLECT COD              ', 'AIR       ',
                          'esias. furiously final foxes detec')],
            'partsupp': [('ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'),
                         (84936, 4937, 8444, 26.97,
                          'riously final instructions. pinto beans cajole. idly even packages haggle doggedly '
                          'furiously regular ')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 3)
        self.assertEqual(len(aoa.arithmetic_eq_predicates), 1)
        self.assertEqual(len(aoa.aoa_predicates), 5)
        self.conn.closeConnection()

    def test_aoa_dev_2(self):
        relations = [self.tab_orders, self.tab_customer, self.tab_nation]
        self.conn.connectUsingParams()

        self.conn.execute_sql([
            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (2900, \'Customer#000002900\', \'xeicQEyv6I\', 9, \'19-292-999-1038\', "
            f"4794.87, \'HOUSEHOLD\', \' ironic packages. pending, regular deposits cajole blithely. carefully even "
            f"instructions engage stealthily carefull\');",

            f"INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)"
            f"VALUES (9, \'INDONESIA\', 2, \' slyly express asymptotes. regular deposits haggle slyly. carefully "
            f"ironic hockey players sleep blithely. carefull\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (55620, 2900, \'O\', 2590.94, \'1998, 1, 10\', \'4-NOT SPECIFIED\', \'Clerk#000000311\', 0, "
            f"\'usly. regular, regul\');"
        ])

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (55620, 2900, 'O', 2590.94, datetime.date(1998, 1, 10), '4-NOT SPECIFIED', 'Clerk#000000311', 0,
             'usly. regular, regul')], 'customer': [
            ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),
            (2900, 'Customer#000002900', 'xeicQEyv6I', 9, '19-292-999-1038', 4794.87, 'HOUSEHOLD',
             ' ironic packages. pending, regular deposits cajole blithely. carefully even '
             'instructions engage stealthily carefull')], 'nation': [
            ('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'),
            (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. '
                                'carefully ironic hockey players sleep blithely. carefull')]}

        low_val = 1000
        high_val = 5527

        query = f"SELECT c_name as name, " \
                f"c_acctbal as account_balance " \
                f"FROM orders, customer, nation " \
                f"WHERE o_custkey > {low_val} and c_custkey = o_custkey and c_custkey <= {high_val}" \
                f"and c_nationkey = n_nationkey " \
                f"and o_orderdate between '1998-01-01' and '1998-01-15' " \
                f"and o_totalprice <= c_acctbal;"

        aoa = AlgebraicPredicate(self.conn, relations, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print("\n=======================")
        print(aoa.where_clause)
        print("=======================")
        self.assertEqual(len(aoa.algebraic_eq_predicates), 2)
        for eq in aoa.algebraic_eq_predicates:
            if len(eq) == 2:
                self.assertEqual(frozenset([('customer', 'c_nationkey'), ('nation', 'n_nationkey')]), frozenset(eq))
            if len(eq) == 3:
                self.assertTrue(('orders', 'o_custkey') in eq)
                self.assertTrue(('customer', 'c_custkey') in eq)
                self.assertEqual(len(eq[2]), 5)
                low = eq[2][3]
                high = eq[2][4]
                self.assertEqual(low, low_val + 1)
                self.assertEqual(high, high_val)
        self.assertEqual(len(aoa.aoa_predicates), 1)
        self.assertTrue((('orders', 'o_totalprice'), ('customer', 'c_acctbal')) in aoa.aoa_predicates)
        self.conn.closeConnection()

    def test_aoa_dev_date_pred(self):
        relations = [self.tab_lineitem, self.tab_orders]
        self.conn.connectUsingParams()

        self.conn.execute_sql([f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
                               f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
                               f"VALUES (2999943, 35074, \'F\', 299094.06, \'1993, 9, 7\', \'3-MEDIUM\', "
                               f"\'Clerk#000000475\', 0,"
                               f"\'hely ironic requests. bold\');",

                               f"Insert into lineitem(l_orderkey, l_partkey,l_suppkey,l_linenumber,l_quantity,"
                               f"l_extendedprice,"
                               f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
                               f"l_shipinstruct,l_shipmode,l_comment)"
                               f" VALUES (2999943, 55442, 4226, 4, 2.00, 2794.88, 0.06, 0.08, \'R\', \'F\', \'1993, "
                               f"10, 15\',"
                               f"\'1993, 10, 24\', \'1993, 11, 1\', \'DELIVER IN PERSON\', \'TRUCK\', \'s. slyly "
                               f"speci\');"
                               ])

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999943, 35074, 'F', 299094.06, datetime.date(1993, 9, 7), '3-MEDIUM', 'Clerk#000000475', 0,
             'hely ironic requests. bold')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                          'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
                         (2999943, 55442, 4226, 4, 2.00, 2794.88, 0.06, 0.08, 'R', 'F', datetime.date(1993, 10, 15),
                          datetime.date(1993, 10, 24), datetime.date(1993, 11, 1), 'DELIVER IN PERSON', 'TRUCK',
                          's. slyly speci')]
        }

        query = "Select o_orderpriority, count(*) as order_count " \
                "From orders, lineitem Where l_orderkey = o_orderkey " \
                "and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01'" \
                " and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority;"

        aoa = AlgebraicPredicate(self.conn, relations, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print("=======================")
        print(aoa.where_clause)
        print("=======================")
        self.assertEqual(len(aoa.algebraic_eq_predicates), 1)
        self.assertTrue(('lineitem', 'l_orderkey') in aoa.algebraic_eq_predicates[0])
        self.assertTrue(('orders', 'o_orderkey') in aoa.algebraic_eq_predicates[0])

        self.assertEqual(len(aoa.arithmetic_ineq_predicates), 0)
        for aoa_ in aoa.aoa_predicates:
            print(aoa_)
        print(len(aoa.aoa_predicates))
        self.assertEqual(len(aoa.aoa_predicates), 3)
        self.assertTrue((('lineitem', 'l_commitdate'), ('lineitem', 'l_receiptdate')) in aoa.aoa_predicates)
        self.assertTrue((datetime.date(1993, 7, 1), ('orders', 'o_orderdate')) in aoa.aoa_predicates)
        self.assertTrue((('orders', 'o_orderdate'), datetime.date(1993, 9, 30)) in aoa.aoa_predicates)

        self.conn.closeConnection()

    def test_aoa_dev_higher_group(self):
        relations = [self.tab_orders, self.tab_customer, self.tab_nation]
        self.conn.connectUsingParams()

        self.conn.execute_sql([
            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (2900, \'Customer#000002900\', \'xeicQEyv6I\', 2900, \'19-292-999-1038\', "
            f"4794.87, \'HOUSEHOLD\', \' ironic packages. pending, regular deposits cajole blithely. carefully even "
            f"instructions engage stealthily carefull\');",

            f"INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)"
            f"VALUES (2900, \'INDONESIA\', 2, \' slyly express asymptotes. regular deposits haggle slyly. carefully "
            f"ironic hockey players sleep blithely. carefull\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (55620, 2900, \'O\', 2590.94, \'1998, 1, 10\', \'4-NOT SPECIFIED\', \'Clerk#000000311\', 0, "
            f"\'usly. regular, regul\');"
        ])

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (55620, 2900, 'O', 2590.94, datetime.date(1998, 1, 10), '4-NOT SPECIFIED', 'Clerk#000000311', 0,
             'usly. regular, regul')], 'customer': [
            ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),
            (2900, 'Customer#000002900', 'xeicQEyv6I', 2900, '19-292-999-1038', 4794.87, 'HOUSEHOLD',
             ' ironic packages. pending, regular deposits cajole blithely. carefully even '
             'instructions engage stealthily carefull')], 'nation': [
            ('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'),
            (2900, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. '
                                   'carefully ironic hockey players sleep blithely. carefull')]}

        low_val = 1000
        high_val = 5527

        query = f"SELECT c_name as name, " \
                f"c_acctbal as account_balance " \
                f"FROM orders, customer, nation " \
                f"WHERE o_custkey > {low_val} and c_custkey = o_custkey and c_custkey <= {high_val}" \
                f"and c_nationkey = n_nationkey " \
                f"and o_orderdate between '1998-01-01' and '1998-01-15' " \
                f"and o_totalprice <= c_acctbal;"

        aoa = AlgebraicPredicate(self.conn, relations, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 2)
        for eq in aoa.algebraic_eq_predicates:
            if len(eq) == 2:
                self.assertEqual(frozenset([('customer', 'c_nationkey'), ('nation', 'n_nationkey')]), frozenset(eq))
            if len(eq) == 3:
                self.assertTrue(('orders', 'o_custkey') in eq)
                self.assertTrue(('customer', 'c_custkey') in eq)
                self.assertEqual(len(eq[2]), 5)
                low = eq[2][3]
                high = eq[2][4]
                self.assertEqual(low, low_val + 1)
                self.assertEqual(high, high_val)

    @pytest.mark.skip
    def test_ordering_problem_aoa_dev(self):
        for i in range(1, 100):
            self.test_aoa_dev_2()

    def test_paritions(self):
        # Example usage
        elements = [1, 2, 3, 4, 5, 6]

        t_all_paritions = merge_equivalent_paritions(elements)
        # Displaying the result
        for i, partition in enumerate(t_all_paritions, 1):
            print(f"Partition {i}: {partition}")

        # self.assertEqual(n, len(t_all_paritions))

    def test_frozenset(self):
        one = frozenset({1, 2})
        two = frozenset({1, 2, 3})
        se = frozenset({one, two})
        i = min(se, key=len)
        self.assertEqual(one, i)

    @pytest.mark.skip
    def test_aoa_bigchain(self):
        query = "select s_name, c_name, n_name from customer, orders, lineitem, supplier, nation " \
                "where c_custkey = o_custkey  " \
                "and o_orderkey = l_orderkey and l_suppkey = s_suppkey " \
                "and s_nationkey = c_nationkey and c_nationkey = n_nationkey;"

    def test_paper_subquery1_projection(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql([
            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (69124, \'Customer#000069124\', \'kMelt4PRpNzF\', 8, \'19-292-999-1038\', "
            f"5988.86, \'FURNITURE\', \' ironic packages. pending, regular deposits cajole blithely. carefully even "
            f"instructions engage stealthily carefull\');",

            f"INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)"
            f"VALUES (8, \'INDIA\', 2, \' slyly express asymptotes. regular deposits haggle slyly. carefully "
            f"ironic hockey players sleep blithely. carefull\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (306560, 69124, \'O\', 5087.46, \'1998, 1, 1\', \'4-NOT SPECIFIED\', \'Clerk#000000311\', 0, "
            f"\'usly. regular, regul\');"
        ])

        self.global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (306560, 69124, 'O', 5087.46, datetime.date(1998, 1, 1), '4-NOT SPECIFIED', 'Clerk#000000311', 0,
             'usly. regular, regul')], 'customer': [
            ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),
            (69124, 'Customer#000069124', 'kMelt4PRpNzF', 8, '19-292-999-1038', 5988.86, 'FURNITURE',
             ' ironic packages. pending, regular deposits cajole blithely. carefully even '
             'instructions engage stealthily carefull')], 'nation': [
            ('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'),
            (8, 'INDIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. '
                            'carefully ironic hockey players sleep blithely. carefull')]}

        self.conn.execute_sql([
            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (69124, \'Customer#000069124\', \'kMelt4PRpNzF\', 8, \'19-292-999-1038\', "
            f"5988.86, \'FURNITURE\', \' ironic packages. pending, regular deposits cajole blithely. carefully even "
            f"instructions engage stealthily carefull\');",

            f"INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)"
            f"VALUES (8, \'INDIA\', 2, \' slyly express asymptotes. regular deposits haggle slyly. carefully "
            f"ironic hockey players sleep blithely. carefull\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (306560, 69124, \'O\', 5087.46, \'1998, 1, 1\', \'4-NOT SPECIFIED\', \'Clerk#000000311\', 0, "
            f"\'usly. regular, regul\');"
        ])

        query, from_rels = get_subquery1()
        self.assertTrue(self.conn.conn is not None)
        aoa = AlgebraicPredicate(self.conn, from_rels, self.global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)

        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        self.assertEqual(len(pj.projected_attribs), 2)
        self.assertEqual(pj.projected_attribs[0], 'c_name')
        self.assertEqual(pj.projected_attribs[1], 'c_acctbal - o_totalprice')
        self.conn.closeConnection()

    def test_UQ11(self):
        self.conn.connectUsingParams()
        query = "Select o_orderpriority, count(*) as order_count From orders, lineitem " \
                "Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' " \
                "and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate " \
                "Group By o_orderpriority " \
                "Order By o_orderpriority;"

        self.conn.execute_sql([
            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2999943, 35074, \'F\', 299094.06, \'1993, 9, 7\', \'3-MEDIUM       \', \'Clerk#000000475\', 0, "
            f"\'hely ironic requests. bold\');",

            f"Insert into lineitem(l_orderkey, l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2999943, 55442, 4226, 4, 2.0, 2794.88, 0.06, 0.08, \'R\', \'F\', \'1993, "
            f"10, 5\',"
            f"\'1993, 10, 24\', \'1993, 11, 1\', \'DELIVER IN PERSON\', \'TRUCK\', \'s. slyly speci\');"
        ])

        self.global_min_instance_dict = \
            {'orders': [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                         'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
                        (2999943, 35074, 'F', 299094.06, datetime.date(1993, 9, 7), '3-MEDIUM       ',
                         'Clerk#000000475', 0, 'hely ironic requests. bold')],
             'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                           'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                           'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode',
                           'l_comment'),
                          (2999943, 55442, 4226, 4, 2.0, 2794.88, 0.06, 0.08, 'R', 'F',
                           datetime.date(1993, 10, 5), datetime.date(1993, 10, 24),
                           datetime.date(1993, 11, 1), 'DELIVER IN PERSON        ', 'TRUCK     ', 's. slyly speci')]}
        from_rels = list(self.global_min_instance_dict.keys())
        print("from_rels: ", from_rels)
        self.assertTrue(self.conn.conn is not None)
        aoa = AlgebraicPredicate(self.conn, from_rels, self.global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.conn.closeConnection()

    def test_paper_subquery2_projection(self):
        self.conn.connectUsingParams()
        self.global_min_instance_dict = {
            'orders': [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                        'o_clerk', 'o_shippriority', 'o_comment'), (
                           2993927, 59983, 'O', 222137.09, datetime.date(1998, 1, 2), '1-URGENT       ',
                           'Clerk#000000800',
                           0,
                           'ges? carefully regular sheaves haggle ')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey',
                          'l_linenumber', 'l_quantity',
                          'l_extendedprice', 'l_discount', 'l_tax',
                          'l_returnflag', 'l_linestatus',
                          'l_shipdate', 'l_commitdate',
                          'l_receiptdate', 'l_shipinstruct',
                          'l_shipmode', 'l_comment'), (
                             2993927, 45499, 4277, 2, 34.0, 49112.66,
                             0.01, 0.06, 'N', 'O',
                             datetime.date(1998, 4, 23),
                             datetime.date(1998, 2, 15),
                             datetime.date(1998, 5, 17),
                             'DELIVER IN PERSON        ', 'TRUCK     ',
                             'ithely final deposits u')],
            'supplier': [('s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'), (
                4277, 'Supplier#000004277       ', 'MPjnMRh5nwI', 1, '11-321-241-8114', 9768.1,
                'final deposits. furiously express instructions boost fluffily around the silent, final packages. ')],
            'nation': [('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'), (1, 'ARGENTINA                ', 1,
                                                                               'al foxes promise slyly according to '
                                                                               'the regular accounts. bold requests '
                                                                               'alon')]}

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey, l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2993927, 45499, 4277, 2, 34.0, 49112.66, 0.01, 0.06, \'N\', \'O\', \'1998, "
            f"4, 23\',"
            f"\'1998, 2, 15\', \'1998, 5, 17\', \'DELIVER IN PERSON\', \'TRUCK\', \'ithely final deposits u\');"
            ,

            f"Insert into supplier(s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment) "
            f"VALUES (4277, \'Supplier#000004277       \', \'MPjnMRh5nwI\', 1, \'11-321-241-8114\', 9768.1, "
            f"\'final deposits. furiously express instructions boost fluffily around the silent, final packages. \');",

            f"INSERT INTO nation(n_nationkey, n_name, n_regionkey, n_comment)"
            f"VALUES (1, \'ARGENTINA\', 1, \' al foxes promise slyly according to the "
            f"regular accounts. bold requests alon\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2993927, 59983, \'O\', 222137.09, \'1998, 1, 2\', "
            f"\'1-URGENT       \', \'Clerk#000000800\', 0, "
            f"\'ges? carefully regular sheaves haggle \');"
        ])

        query, from_rels = get_subquery2()
        self.assertTrue(self.conn.conn is not None)
        aoa = AlgebraicPredicate(self.conn, from_rels, self.global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        self.conn.closeConnection()

    def test_UQ10(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem']
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate ;"

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (3000000,83848, 1381, 5, 5.0, 9159.2, 0.09, 0.07, \'N\', \'O\', \'1995, 7, 25\',"
            f"\'1995, 7, 26\', \'1995, 7, 27\',\'NONE              \', \'TRUCK       \',"
            f"\'ecial packages haggle furious\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (3000000, 47480, \'O\', 189194.91, \'1995, 6, 20\', \'1-URGENT\', \'Clerk#000000149\', 0, "
            f"\'lly special ideas maintain furiously special requests. furio\');"
        ])

        global_min_instance_dict = \
            {'orders': [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                         'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
                        (3000000, 47480, 'O', 189194.91, datetime.date(1995, 6, 20),
                         '1-URGENT       ', 'Clerk#000000149', 0,
                         'lly special ideas maintain furiously special requests. furio')],
             'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                           'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                           'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                           'l_shipmode', 'l_comment'),
                          (3000000, 83848, 1381, 5, 5.0, 9159.2, 0.09, 0.07, 'N', 'O',
                           datetime.date(1995, 7, 25), datetime.date(1995, 7, 26),
                           datetime.date(1995, 7, 27), 'NONE                     ', 'TRUCK     ',
                           'ecial packages haggle furious')]}
        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 1)
        self.assertEqual(len(aoa.arithmetic_eq_predicates), 0)
        self.assertEqual(len(aoa.aoa_predicates), 0)
        self.assertEqual(len(aoa.aoa_less_thans), 1)

        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(self.global_min_instance_dict)
        self.assertTrue(check)
        self.assertEqual(len(pj.projected_attribs), 1)
        self.assertEqual(pj.projected_attribs[0], 'l_shipmode')
        self.conn.closeConnection()

    def test_UQ10_2(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem']
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate " \
                "and l_receiptdate >= '1993-01-01' and " \
                "l_receiptdate < '1995-01-01';"

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (3000000,83848, 1381, 5, 5.0, 9159.2, 0.09, 0.07, \'N\', \'O\', \'1994, 7, 25\',"
            f"\'1994, 7, 26\', \'1994, 7, 27\',\'NONE              \', \'TRUCK       \',"
            f"\'ecial packages haggle furious\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (3000000, 47480, \'O\', 189194.91, \'1995, 6, 20\', \'1-URGENT\', \'Clerk#000000149\', 0, "
            f"\'lly special ideas maintain furiously special requests. furio\');"
        ])

        global_min_instance_dict = {
            'orders': [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                        'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
                       (3000000, 47480, 'O', 189194.91, datetime.date(1995, 6, 20),
                        '1-URGENT       ', 'Clerk#000000149', 0,
                        'lly special ideas maintain furiously special requests. furio')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                          'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                          'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                          'l_shipmode', 'l_comment'), (3000000, 83848, 1381, 5, 5.0, 9159.2, 0.09, 0.07,
                                                       'N', 'O', datetime.date(1995, 7, 25),
                                                       datetime.date(1995, 7, 26), datetime.date(1995, 7, 27),
                                                       'NONE                     ', 'TRUCK     ',
                                                       'ecial packages haggle furious')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True

        res = aoa.app.doJob(query)
        self.assertFalse(isQ_result_empty(res))

        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(4, aoa.where_clause.count("and"))

        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(delivery.global_min_instance_dict)
        self.assertTrue(check)
        self.conn.closeConnection()

    def test_UQ10_1(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem']
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate;"

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (3000000,83848, 1381, 5, 5.0, 9159.2, 0.09, 0.07, \'N\', \'O\', \'1995, 7, 25\',"
            f"\'1995, 7, 26\', \'1995, 7, 27\',\'NONE              \', \'TRUCK       \',"
            f"\'ecial packages haggle furious\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (3000000, 47480, \'O\', 189194.91, \'1995, 6, 20\', \'1-URGENT\', \'Clerk#000000149\', 0, "
            f"\'lly special ideas maintain furiously special requests. furio\');"
        ])

        global_min_instance_dict = {
            'orders': [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                        'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
                       (3000000, 47480, 'O', 189194.91, datetime.date(1995, 6, 20),
                        '1-URGENT       ', 'Clerk#000000149', 0,
                        'lly special ideas maintain furiously special requests. furio')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                          'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                          'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                          'l_shipmode', 'l_comment'), (3000000, 83848, 1381, 5, 5.0, 9159.2, 0.09, 0.07,
                                                       'N', 'O', datetime.date(1995, 7, 25),
                                                       datetime.date(1995, 7, 26), datetime.date(1995, 7, 27),
                                                       'NONE                     ', 'TRUCK     ',
                                                       'ecial packages haggle furious')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 1)
        self.assertEqual(len(aoa.arithmetic_eq_predicates), 0)
        self.assertEqual(len(aoa.aoa_predicates), 0)
        self.assertEqual(len(aoa.aoa_less_thans), 2)

        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(delivery.global_min_instance_dict)
        self.assertTrue(check)
        self.conn.closeConnection()

    def test_UQ13(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem', 'partsupp']
        query = "Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where " \
                "o_orderkey = l_orderkey and " \
                "ps_partkey = l_partkey and " \
                "ps_suppkey = l_suppkey and " \
                "ps_availqty = l_linenumber and " \
                "l_shipdate >= o_orderdate and " \
                "o_orderdate >= '1990-01-01' and " \
                "l_commitdate <= l_receiptdate and " \
                "l_shipdate <= l_commitdate and " \
                "l_receiptdate > '1994-01-01' " \
                "Order By l_orderkey Limit 7;"

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2688869,24707, 4708, 2, 15.0, 24475.50, 0.04, 0.04, \'A\', \'F\', \'1995, 3, 9\',"
            f"\'1995, 3, 13\', \'1995, 4, 4\',\'COLLECT COD              \', \'TRUCK       \',"
            f"\'ckly bold att\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2688869, 17513, \'F\', 258583.28, \'1995, 1, 14\', \'3-MEDIUM\', \'Clerk#000000672\', 0, "
            f"\'ts. slyly regular escapades boost \');",

            f"Insert into partsupp(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) "
            f"VALUES (24707, 4708, 2, 788.97, \'ounts. blithely express platelets according to the \');"
        ])

        global_min_instance_dict = {
            'orders': [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                        'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
                       (2688869, 17513, 'F', 258583.28, datetime.date(1995, 1, 14),
                        '3-MEDIUM       ', 'Clerk#000000672', 0,
                        'ts. slyly regular escapades boost ')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                          'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                          'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                          'l_shipmode', 'l_comment'), (2688869, 24707, 4708, 2, 15.00, 24475.50, 0.04, 0.04, 'A',
                                                       'F', datetime.date(1995, 3, 9), datetime.date(1995, 3, 13),
                                                       datetime.date(1995, 4, 4),
                                                       'COLLECT COD              ', 'TRUCK     ', 'ckly bold att')],
            'partsupp': [('ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'),
                         (24707, 4708, 2, 788.97, 'ounts. blithely express platelets according to the ')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(len(aoa.aoa_predicates), 5)
        self.assertEqual(len(aoa.aoa_less_thans), 0)
        self.assertEqual(8, aoa.where_clause.count("and"))

        delivery = aoa.pipeline_delivery
        pj = Projection(self.conn, delivery)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(delivery.global_min_instance_dict)
        self.assertTrue(check)
        print(pj.projected_attribs)
        print(pj.projection_names)
        self.conn.closeConnection()

    def test_order_by(self):
        self.conn.connectUsingParams()
        core_rels = ['orders', 'lineitem', 'part']

        global_min_instant_dict = {'orders':
                                       [('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                                         'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
                                        (2940454, 134431, 'O', 216011.31, datetime.date(1996, 2, 15), '2-HIGH         ',
                                         'Clerk#000000781',
                                         0, ' accounts haggle above the slyly si')],
                                   'part': [("p_partkey",
                                             "p_name",
                                             "p_mfgr",
                                             "p_brand",
                                             "p_type",
                                             "p_size",
                                             "p_container",
                                             "p_retailprice",
                                             "p_comment"),
                                            (280, 'blue azure slate lace burlywood', 'Manufacturer#2           ',
                                             'Brand#21  ',
                                             'STANDARD BURNISHED STEEL', 33, 'LG CAN    ', 1180.28, 'egular, s')],
                                   'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                                                 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag',
                                                 'l_linestatus',
                                                 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                                                 'l_shipmode', 'l_comment'), (
                                                    2940454, 280, 4031, 2, 1.00, 1180.28, 0.05, 0.02, 'N', 'O',
                                                    datetime.date(1996, 4, 9), datetime.date(1996, 4, 20),
                                                    datetime.date(1996, 4, 12),
                                                    'TAKE BACK RETURN         ', 'RAIL      ', 'sts. final')]}

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2940454, 280, 4031, 2, 1.00, 1180.28, 0.05, 0.02, \'N\', \'O\', \'1996, 4, 9\',"
            f"\'1996, 4, 20\', \'1996, 4, 12\',\'TAKE BACK RETURN              \', \'RAIL       \',"
            f"\'sts. final\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2940454, 134431, \'O\', 216011.31, \'1996, 2, 15\', \'2-HIGH\', \'Clerk#000000781\', 0, "
            f"\' accounts haggle above the slyly si\');",

            f"Insert into part(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, "
            f"p_comment)"
            f"VALUES (280, \'blue azure slate lace burlywood\', \'Manufacturer#2           \', \'Brand#21  \', "
            f"\'STANDARD BURNISHED STEEL\', 33, \'LG CAN    \', 1180.28, \'egular, s\');"
        ])

        query = "Select p_brand, o_clerk, l_shipmode " \
                "From orders, lineitem, part " \
                "Where l_partkey = p_partkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate " \
                "and o_orderdate > '1994-01-01' " \
                "and l_shipdate > '1995-01-01' " \
                "and p_retailprice >= l_extendedprice " \
                "and p_partkey < 10000 " \
                "and l_suppkey < 10000 " \
                "and p_container = 'LG CAN' " \
                "Order By o_clerk LIMIT 10;"

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instant_dict)
        aoa.mock = True
        res = aoa.app.doJob(query)
        self.assertFalse(isQ_result_empty(res))
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertEqual(8, aoa.where_clause.count("and"))

        pj = Projection(self.conn, aoa.pipeline_delivery)
        check = pj.doJob(query)
        self.assertTrue(check)
        self.assertEqual(3, len(pj.projected_attribs))

        ob = OrderBy(self.conn, pj.projected_attribs, pj.projection_names, pj.dependencies,
                     [('p_brand', ''), ('o_clerk', ''), ('l_shipmode', '')], aoa.pipeline_delivery)
        ob.doJob(query)
        self.assertTrue(ob.has_orderBy)
        self.assertTrue("o_clerk" in ob.orderBy_string)
        print(ob.orderBy_string)
        print(ob.orderby_list)

        lm = Limit(self.conn, [], aoa.pipeline_delivery)
        lm.doJob(query)
        print(lm.limit)
        self.assertTrue(lm.limit is not None)

        self.conn.closeConnection()
