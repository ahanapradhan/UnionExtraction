import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.from_clause import FromClause
from mysite.unmasque.test import queries


class MyTestCase(unittest.TestCase):

    def test_like_tpchq1(self):
        query = queries.tpch_query1
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob([query, "error"])
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)
        conn.closeConnection()

    def test_like_tpchq3(self):
        query = queries.tpch_query3
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob([query, "error"])
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('customer' in rels)
        self.assertTrue('orders' in rels)
        conn.closeConnection()

    def test_no_tables_in_db(self):
        query = "select count(*) from lineitem"
        conn = ConnectionHelper("postgres", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob([query, "error"])
        self.assertTrue(not rels)
        conn.closeConnection()

    def test_unionQ(self):
        query = "(select l_partkey as key from lineitem limit 2) union all (select p_partkey as key from part limit 2)"
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob([query, "error"])
        self.assertEqual(len(rels), 2)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('part' in rels)
        conn.closeConnection()

    def test_unionQ2(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey limit 2) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey limit 2)"

        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob([query, "error"])
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('part' in rels)
        self.assertTrue('orders' in rels)

        rels = fc.doJob([query, "rename"])
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)

        conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
