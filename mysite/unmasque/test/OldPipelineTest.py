import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.src.core.UN1_from_clause import UN1FromClause


class MyTestCase(unittest.TestCase):
    def test_basic_flow(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey limit 2) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey limit 2)"

        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = UN1FromClause(conn)
        partials = fc.get_partial_QH(query)
        self.assertEqual(partials, {'part', 'orders'})
        ftabs = fc.get_fromTabs(query)
        self.assertEqual(len(ftabs), 3)
        ctabs = fc.get_comTabs(query)
        self.assertEqual(len(ctabs), 1)


if __name__ == '__main__':
    unittest.main()
