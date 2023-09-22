import sys
import unittest

sys.path.append("../../../")

from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from mysite.unmasque.refactored.from_clause import FromClause
from mysite.unmasque.test.util import queries


class MyTestCase(BaseTestCase):

    def test_like_tpchq1(self):
        query = queries.tpch_query1
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        print("Rels", rels)
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)
        self.conn.closeConnection()

    def test_like_tpchq3(self):
        query = queries.tpch_query3
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('customer' in rels)
        self.assertTrue('orders' in rels)
        self.conn.closeConnection()

    def test_no_tables_in_db(self):
        query = "select count(*) from lineitem"
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        self.assertTrue(not rels)
        self.conn.closeConnection()

    def test_unionQ(self):
        query = "(select l_partkey as key from lineitem limit 2) union all (select p_partkey as key from part limit 2)"
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        self.assertEqual(len(rels), 2)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('part' in rels)
        self.conn.closeConnection()

    def test_unionQ2(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey limit 2) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey limit 2)"

        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('part' in rels)
        self.assertTrue('orders' in rels)

        rels = fc.doJob([query, "rename"])
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
