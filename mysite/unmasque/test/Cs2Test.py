import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.test import queries


class MyTestCase(unittest.TestCase):
    relations = ["orders", "lineitem", "customer", "supplier", "part", "partsupp", "nation", "region"]

    key_lists = [[('part', 'p_partkey'), ('partsupp', 'ps_partkey'), ('lineitem', 'l_partkey')],
                 [('supplier', 's_suppkey'), ('partsupp', 'ps_suppkey'), ('lineitem', 'l_suppkey')],
                 [('supplier', 's_nationkey'), ('nation', 'n_nationkey'), ('customer', 'c_nationkey')],
                 [('customer', 'c_custkey'), ('orders', 'o_custkey')],
                 [('orders', 'o_orderkey'), ('lineitem', 'l_orderkey')],
                 [('region', 'r_regionkey'), ('nation', 'n_regionkey')]]

    def test_single_table_cs2(self):
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)

        from_rels = ["lineitem"]
        cs2 = Cs2(conn, self.relations, from_rels, self.key_lists)
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
        cs2 = Cs2(conn, self.relations, from_rels, self.key_lists)
        sizes = cs2.getSizes_cs()
        self.assertEqual(len(sizes), 8)
        cs2.doJob(queries.tpch_query3)
        self.assertEqual(cs2.status, "PASS")  # add assertion here
        for fromr in from_rels:
            print(cs2.sample[fromr], sizes[fromr])
            self.assertTrue(cs2.sample[fromr] < sizes[fromr])


if __name__ == '__main__':
    unittest.main()
