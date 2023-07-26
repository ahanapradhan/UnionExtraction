import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.test import queries, tpchSettings


class MyTestCase(unittest.TestCase):

    def test_single_table_cs2(self):
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)

        from_rels = ["lineitem"]
        cs2 = Cs2(conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        sizes = cs2.getSizes_cs()
        self.assertEqual(len(sizes), 8)
        cs2.doJob(queries.tpch_query1)
        self.assertEqual(cs2.status, "PASS")  # add assertion here
        for fromr in from_rels:
            self.assertTrue(cs2.sample[fromr] < sizes[fromr])

    def test_multiple_tables_cs2(self):
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)

        from_rels = ["lineitem", "orders", "customer"]
        cs2 = Cs2(conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        sizes = cs2.getSizes_cs()
        self.assertEqual(len(sizes), 8)
        cs2.doJob(queries.tpch_query3)
        self.assertEqual(cs2.status, "PASS")  # add assertion here
        for fromr in from_rels:
            print(cs2.sample[fromr], sizes[fromr])
            self.assertTrue(cs2.sample[fromr] < sizes[fromr])


if __name__ == '__main__':
    unittest.main()
