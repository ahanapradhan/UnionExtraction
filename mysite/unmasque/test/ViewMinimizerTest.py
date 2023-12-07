import unittest

from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.refactored.view_minimizer import ViewMinimizer
from mysite.unmasque.test.util import tpchSettings, queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase

@unittest.skip("Skipping this test class as the class is deprecated.")
class MyTestCase(BaseTestCase):

    def test_for_cs2_pass_single_table(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        from_rels = tpchSettings.from_rels['tpch_query1']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.tpch_query1)
        self.assertTrue(cs2.passed)
        minimizer = ViewMinimizer(self.conn, from_rels, cs2.sizes, cs2.passed)
        check = minimizer.doJob(queries.tpch_query1)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.global_min_instance_dict["lineitem"]), 2)
        self.assertTrue(len(minimizer.global_result_dict['min']) > 0)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        self.assertTrue(len(minimizer.global_other_info_dict['min']) > 0)
        self.conn.closeConnection()

    def test_for_cs2_pass_tpch_query3(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['tpch_query3']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.tpch_query3)
        self.assertTrue(cs2.passed)
        minimizer = ViewMinimizer(self.conn, from_rels, cs2.sizes, cs2.passed)
        check = minimizer.doJob(queries.tpch_query3)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_pass_Q1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q1']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.Q1)
        self.assertTrue(cs2.passed)
        minimizer = ViewMinimizer(self.conn, from_rels, cs2.sizes, cs2.passed)
        check = minimizer.doJob(queries.Q1)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_fail_Q1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q1']

        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q1)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_pass_Q2(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q2']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.Q2)
        self.assertTrue(cs2.passed)
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, cs2.passed)
        check = minimizer.doJob(queries.Q2)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_pass_Q3(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.Q3)
        self.assertTrue(cs2.passed)
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, cs2.passed)
        check = minimizer.doJob(queries.Q3)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_pass_Q11(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q11']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.Q11)
        self.assertTrue(cs2.passed)

        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, cs2.passed)
        check = minimizer.doJob(queries.Q11)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_fail_Q16(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q16']

        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q16)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_fail_Q17(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q17']

        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.Q11)
        self.assertTrue(cs2.passed)

        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, cs2.passed)
        check = minimizer.doJob(queries.Q17)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    def test_for_cs2_fail_Q18(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q18']

        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q18)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()

    """
    def test_for_cs2_fail_dup(self):
        hq = "select o_orderdate, l_extendedprice, n2.n_name " \
             "from part, supplier, lineitem, orders, customer, nation n1, nation n2, region " \
             "where p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND " \
             "o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND " \
             "s_nationkey = n2.n_nationkey AND r_name = 'AMERICA' " \
             "AND p_type = 'ECONOMY ANODIZED STEEL' AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31';"

        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = ['part', 'supplier', 'lineitem', 'orders', 'customer', 'nation', 'region']

        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(hq)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()
    """


if __name__ == '__main__':
    unittest.main()
