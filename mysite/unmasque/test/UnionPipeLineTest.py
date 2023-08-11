import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.src.core import UnionPipeLine, algorithm1
from mysite.unmasque.src.core.UN1_from_clause import UN1FromClause
from mysite.unmasque.test import queries


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_nonUnion_query(self):
        key = 'tpch_query1'
        query = queries.queries_dict[key]
        u_Q, tp = UnionPipeLine.extract(self.conn, query)
        self.assertTrue(u_Q is not None)
        print(u_Q)
        tp.print()

    def test_nonUnion_query_Q2(self):
        key = 'Q3'
        query = queries.queries_dict[key]
        u_Q, tp = UnionPipeLine.extract(self.conn, query)
        self.assertTrue(u_Q is not None)
        print(u_Q)
        tp.print()

    def test_nonUnion_queries(self):
        Q_keys = queries.queries_dict.keys()
        f = open("UnionPipeLineTest_results.txt.txt", "w")
        q_no = 1
        for q_key in Q_keys:
            query = queries.queries_dict[q_key]
            u_Q, tp = UnionPipeLine.extract(self.conn, query)
            self.assertTrue(u_Q is not None)
            print(u_Q)
            f.write("\n" + str(q_no) + ":")
            f.write("\tHidden Query:\n")
            f.write(query)
            f.write("\n*** Extracted Query:\n")
            f.write(u_Q)
            f.write("\n---------------------------------------\n")
            tp.print()
            q_no += 1
        f.close()

    def test_unionQ(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= " \
                "905) " \
                "union all " \
                "(select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);"
        u_Q, tp = UnionPipeLine.extract(self.conn, query)
        self.assertTrue(u_Q is not None)
        print(u_Q)
        f = open("check.txt", 'w')
        f.write(query + "\n\n")
        f.write(u_Q)
        f.close()
        tp.print()

    def test_unionQuery_ui_caught_case(self):
        self.conn.connectUsingParams()
        query = "(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and " \
                "n_name = 'UNITED STATES') UNION ALL " \
                "(SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey " \
                "and l_quantity > 35);"

        db = UN1FromClause(self.conn)
        p, pstr = algorithm1.algo(db, query)
        self.assertEqual(p, {frozenset({'customer', 'nation'}), frozenset({'part', 'lineitem'})})
        self.assertTrue(pstr is not None)
        self.conn.closeConnection()

    def test_random_nonUnion(self):
        query = "SELECT o_orderdate, SUM(l_extendedprice) AS total_price " \
                "FROM orders, lineitem where o_orderkey = l_orderkey " \
                "and o_orderdate >= '1995-01-01' GROUP BY o_orderdate " \
                "ORDER BY total_price DESC LIMIT 10;"
        eq = UnionPipeLine.extract(self.conn, query)
        print(eq)


if __name__ == '__main__':
    unittest.main()
