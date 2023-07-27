import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.view_minimizer import ViewMinimizer
from mysite.unmasque.refactored.where_clause import WhereClause
from mysite.unmasque.test import tpchSettings, queries


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_init_data(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['tpch_query1']
        minimizer = ViewMinimizer(self.conn, from_rels, False)
        check = minimizer.doJob(queries.tpch_query1)
        self.assertTrue(check)

        wc = WhereClause(self.conn, tpchSettings.key_lists, from_rels,
                         minimizer.global_other_info_dict, minimizer.global_result_dict,
                         minimizer.global_min_instance_dict)

        self.assertEqual(wc.global_attrib_types, [])
        self.assertEqual(wc.global_all_attribs, [])
        self.assertEqual(wc.global_d_plus_value, {})
        self.assertEqual(wc.global_attrib_max_length, {})

        wc.get_init_data()

        self.assertEqual(set(wc.global_attrib_types),
                         {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                          ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                          ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                          ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                          ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                          ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                          ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                          ('lineitem', 'l_shipmode', 'character'), ('lineitem', 'l_comment', 'character varying'),
                          ('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                          ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                          ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                          ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                          ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                          ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                          ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                          ('lineitem', 'l_shipmode', 'character'), ('lineitem', 'l_comment', 'character varying')})

        self.assertEqual(len(wc.global_all_attribs[0]), 16)
        self.assertEqual(len(wc.global_attrib_max_length), 5)
        self.assertEqual(len(wc.global_d_plus_value), 16)
        self.conn.closeConnection()

    def test_join_graph(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q17']
        minimizer = ViewMinimizer(self.conn, from_rels, False)
        check = minimizer.doJob(queries.Q17)
        self.assertTrue(check)

        wc = WhereClause(self.conn, tpchSettings.key_lists, from_rels,
                         minimizer.global_other_info_dict, minimizer.global_result_dict,
                         minimizer.global_min_instance_dict)

        wc.get_init_data()
        self.assertEqual(len(wc.global_all_attribs), 2)

        wc.get_join_graph(queries.Q17)
        self.assertEqual(len(wc.global_join_graph), 1)
        self.assertEqual(set(wc.global_join_graph[0]), {'p_partkey', 'l_partkey'})
        self.assertEqual(len(wc.global_key_attributes), 2)
        self.assertTrue('p_partkey' in wc.global_key_attributes)
        self.assertTrue('l_partkey' in wc.global_key_attributes)


if __name__ == '__main__':
    unittest.main()
