import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.refactored.view_minimizer import ViewMinimizer
from mysite.unmasque.test import queries, tpchSettings


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_for_cs2_pass_single_table(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        from_rels = tpchSettings.from_rels['tpch_query1']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.tpch_query1)
        self.assertEqual(cs2.status, "PASS")
        minimizer = ViewMinimizer(self.conn, from_rels, cs2.status)
        check = minimizer.doJob(queries.tpch_query1)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.global_min_instance_dict["lineitem"]), 2)
        self.assertTrue(len(minimizer.global_result_dict['min']) > 0)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        self.assertTrue(len(minimizer.global_other_info_dict['min']) > 0)
        self.conn.closeConnection()

    def test_for_cs2_pass_tpch_query3(self):
        self.conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['tpch_query3']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        cs2.doJob(queries.tpch_query3)
        self.assertEqual(cs2.status, "PASS")

        minimizer = ViewMinimizer(self.conn, from_rels, cs2.status)
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
        self.assertEqual(cs2.status, "PASS")

        minimizer = ViewMinimizer(self.conn, from_rels, cs2.status)
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
        self.assertEqual(cs2.status, "PASS")

        minimizer = ViewMinimizer(self.conn, from_rels, cs2.status)
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
        self.assertEqual(cs2.status, "PASS")

        minimizer = ViewMinimizer(self.conn, from_rels, cs2.status)
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
        self.assertEqual(cs2.status, "PASS")

        minimizer = ViewMinimizer(self.conn, from_rels, cs2.status)
        check = minimizer.doJob(queries.Q11)
        self.assertTrue(check)
        self.assertEqual(len(minimizer.local_other_info_dict['Result Cardinality']), 1)
        for tab in from_rels:
            self.assertEqual(len(minimizer.global_min_instance_dict[tab]), 2)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
