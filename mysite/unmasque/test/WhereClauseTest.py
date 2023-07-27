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
        self.assertEqual(len(wc.global_all_attribs), 2)  # per table 1 attrib, Q17 has 2 tables

        wc.get_join_graph(queries.Q17)
        self.assertEqual(len(wc.global_join_graph), 1)
        self.assertEqual(set(wc.global_join_graph[0]), {'p_partkey', 'l_partkey'})
        self.assertEqual(len(wc.global_key_attributes), 2)
        self.assertTrue('p_partkey' in wc.global_key_attributes)
        self.assertTrue('l_partkey' in wc.global_key_attributes)
        self.conn.closeConnection()

    def test_join_graph1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q21']
        minimizer = ViewMinimizer(self.conn, from_rels, False)
        check = minimizer.doJob(queries.Q21)
        self.assertTrue(check)

        wc = WhereClause(self.conn, tpchSettings.key_lists, from_rels,
                         minimizer.global_other_info_dict, minimizer.global_result_dict,
                         minimizer.global_min_instance_dict)

        wc.get_init_data()
        self.assertEqual(len(wc.global_all_attribs), 4)  # per table 1 attrib, Q17 has 2 tables

        wc.get_join_graph(queries.Q21)
        self.assertEqual(len(wc.global_join_graph), 3)
        join_edges = frozenset({frozenset({'l_suppkey', 's_suppkey'}),
                                frozenset({'l_orderkey', 'o_orderkey'}),
                                frozenset({'s_nationkey', 'n_nationkey'})})
        for join_edge in wc.global_join_graph:
            edge = frozenset({join_edge[0], join_edge[1]})
            self.assertTrue(edge in join_edges)

        self.assertEqual(len(wc.global_key_attributes), 6)
        self.assertTrue('l_suppkey' in wc.global_key_attributes)
        self.assertTrue('s_suppkey' in wc.global_key_attributes)
        self.assertTrue('l_orderkey' in wc.global_key_attributes)
        self.assertTrue('o_orderkey' in wc.global_key_attributes)
        self.assertTrue('s_nationkey' in wc.global_key_attributes)
        self.assertTrue('n_nationkey' in wc.global_key_attributes)
        self.conn.closeConnection()

    def test_join_graph2(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q23_1']
        minimizer = ViewMinimizer(self.conn, from_rels, False)
        check = minimizer.doJob(queries.Q23_1)
        self.assertTrue(check)

        wc = WhereClause(self.conn, tpchSettings.key_lists, from_rels,
                         minimizer.global_other_info_dict, minimizer.global_result_dict,
                         minimizer.global_min_instance_dict)

        wc.get_init_data()
        self.assertEqual(len(wc.global_all_attribs), 4)  # per table 1 attrib, Q17 has 2 tables

        wc.get_join_graph(queries.Q23_1)
        self.assertEqual(len(wc.global_join_graph), 3)
        join_edges = frozenset({frozenset({'ps_suppkey', 's_suppkey'}),
                                frozenset({'n_regionkey', 'r_regionkey'}),
                                frozenset({'s_nationkey', 'n_nationkey'})})
        print(wc.global_join_graph)
        for join_edge in wc.global_join_graph:
            edge = frozenset({join_edge[0], join_edge[1]})
            self.assertTrue(edge in join_edges)

        self.assertEqual(len(wc.global_key_attributes), 6)
        self.assertTrue('ps_suppkey' in wc.global_key_attributes)
        self.assertTrue('s_suppkey' in wc.global_key_attributes)
        self.assertTrue('s_nationkey' in wc.global_key_attributes)
        self.assertTrue('n_nationkey' in wc.global_key_attributes)
        self.assertTrue('r_regionkey' in wc.global_key_attributes)
        self.assertTrue('n_regionkey' in wc.global_key_attributes)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
