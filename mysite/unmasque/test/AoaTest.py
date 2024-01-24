import datetime

from mysite.unmasque.refactored.aoa import merge_equivalent_paritions, AlgebraicPredicate
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
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

        aoa = AlgebraicPredicate(self.conn, None, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 3)
        self.assertEqual(len(aoa.arithmetic_eq_predicates), 1)

        print(aoa.where_clause)

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

        aoa = AlgebraicPredicate(self.conn, None, relations, global_min_instance_dict)
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
        self.assertEqual(len(aoa.aoa_predicates), 1)
        self.assertTrue((('orders', 'o_totalprice'), ('customer', 'c_acctbal')) in aoa.aoa_predicates)
        print("=======================")
        print(aoa.where_clause)
        print("=======================")
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

        aoa = AlgebraicPredicate(self.conn, None, relations, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        self.assertEqual(len(aoa.algebraic_eq_predicates), 1)
        self.assertTrue(('lineitem', 'l_orderkey') in aoa.algebraic_eq_predicates[0])
        self.assertTrue(('orders', 'o_orderkey') in aoa.algebraic_eq_predicates[0])

        self.assertEqual(len(aoa.arithmetic_ineq_predicates), 0)

        self.assertEqual(len(aoa.aoa_predicates), 3)
        self.assertTrue((('lineitem', 'l_commitdate'), ('lineitem', 'l_receiptdate')) in aoa.aoa_predicates)
        self.assertTrue([datetime.date(1993, 7, 1), ('orders', 'o_orderdate')] in aoa.aoa_predicates)
        self.assertTrue([('orders', 'o_orderdate'), datetime.date(1993, 9, 30)] in aoa.aoa_predicates)
        print("=======================")
        print(aoa.where_clause)
        print("=======================")

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

        aoa = AlgebraicPredicate(self.conn, None, relations, global_min_instance_dict)
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

    def test_aoa_bigchain(self):
        query = "select s_name, c_name, n_name from customer, orders, lineitem, supplier, nation " \
                "where c_custkey = o_custkey  " \
                "and o_orderkey = l_orderkey and l_suppkey = s_suppkey " \
                "and s_nationkey = c_nationkey and c_nationkey = n_nationkey;"
