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

    def test_aoa_dev(self):
        query = "SELECT c_name as name, " \
                "c_acctbal as account_balance " \
                "FROM orders, customer, nation " \
                "WHERE o_custkey > 2500 and c_custkey = o_custkey and c_custkey <= 5000" \
                "and c_nationkey = n_nationkey " \
                "and o_orderdate between '1998-01-01' and '1998-01-15' " \
                "and o_totalprice <= c_acctbal;"
        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq)
        # self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_paritions(self):
        # Example usage
        elements = [1, 2, 3, 4]

        t_all_paritions = merge_equivalent_paritions(elements)
        # Displaying the result
        for i, partition in enumerate(t_all_paritions, 1):
            print(f"Partition {i}: {partition}")

        # self.assertEqual(n, len(t_all_paritions))
