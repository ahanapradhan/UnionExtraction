import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.test import queries, tpchSettings


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_single_table_cs2(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['tpch_query1']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        sizes = cs2.getSizes_cs()
        self.assertEqual(len(sizes), 8)
        cs2.doJob(queries.tpch_query1)
        self.assertTrue(cs2.passed)
        for fromr in from_rels:
            self.assertTrue(cs2.sample[fromr] < sizes[fromr])
        self.conn.closeConnection()

    def test_multiple_tables_cs2(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['tpch_query3']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        sizes = cs2.getSizes_cs()
        self.assertEqual(len(sizes), 8)
        cs2.doJob(queries.tpch_query3)
        self.assertTrue(cs2.passed)
        for fromr in from_rels:
            print(cs2.sample[fromr], sizes[fromr])
            self.assertTrue(cs2.sample[fromr] < sizes[fromr])
        self.conn.closeConnection()

    def test_multiple_tables2_cs2(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q17']
        cs2 = Cs2(self.conn, tpchSettings.relations, from_rels, tpchSettings.key_lists)
        sizes = cs2.getSizes_cs()
        self.assertEqual(len(sizes), 8)
        cs2.doJob(queries.Q17)
        self.assertTrue(cs2.passed)
        for fromr in from_rels:
            print(cs2.sample[fromr], sizes[fromr])
            self.assertTrue(cs2.sample[fromr] < sizes[fromr])
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
