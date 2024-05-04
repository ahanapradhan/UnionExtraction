import copy
import unittest

from mysite.unmasque.src.core.db_restorer import DbRestorer
from mysite.unmasque.src.core.equi_join import U2EquiJoin
from mysite.unmasque.src.core.from_clause import FromClause
from mysite.unmasque.src.obsolete.equi_join import EquiJoin
from mysite.unmasque.src.core.filter import Filter
from mysite.unmasque.src.core.view_minimizer import ViewMinimizer
from mysite.unmasque.src.util.constants import max_int_val
from ..test.util import queries, tpchSettings
from ..test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_init_data(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['tpch_query1']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.tpch_query1)
        self.assertTrue(check)

        wc = EquiJoin(self.conn, tpchSettings.key_lists, from_rels,
                      minimizer.global_min_instance_dict)

        self.assertEqual(wc.global_attrib_types, [])
        self.assertEqual(wc.global_all_attribs, [])
        self.assertEqual(wc.global_d_plus_value, {})
        self.assertEqual(wc.global_attrib_max_length, {})

        wc.do_init()

        self.assertEqual(set(wc.global_attrib_types),
                         {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
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
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q17)
        self.assertTrue(check)

        wc = EquiJoin(self.conn, tpchSettings.key_lists, from_rels,
                      minimizer.global_min_instance_dict)

        wc.doJob(queries.Q17)
        self.assertEqual(len(wc.global_all_attribs), 2)  # per table 1 attrib, Q17 has 2 tables
        self.assertEqual(len(wc.global_join_graph), 1)
        self.assertEqual(set(wc.global_join_graph[0]), {'p_partkey', 'l_partkey'})
        self.assertEqual(len(wc.global_key_attributes), 2)
        self.assertTrue('p_partkey' in wc.global_key_attributes)
        self.assertTrue('l_partkey' in wc.global_key_attributes)

        wc = Filter(self.conn, from_rels,
                    minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q17)
        print(filters)
        self.assertEqual(len(filters), 2)

        f = filters[0]
        self.assertEqual(f[0], 'part')
        self.assertEqual(f[1], 'p_brand')
        self.assertEqual(f[2], 'equal')
        self.assertTrue('Brand#52' in f[3])

        f = filters[1]
        self.assertEqual(f[0], 'part')
        self.assertEqual(f[1], 'p_container')
        self.assertEqual(f[2], 'equal')
        self.assertTrue('LG CAN' in f[3])
        self.conn.closeConnection()

    def test_join_graph1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q21']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q21)
        self.assertTrue(check)

        wc = EquiJoin(self.conn, tpchSettings.key_lists, from_rels,
                      minimizer.global_min_instance_dict)

        wc.doJob(queries.Q21)
        self.assertEqual(len(wc.global_all_attribs), 4)  # per table 1 attrib, Q17 has 2 tables
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
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q23_1)
        self.assertTrue(check)

        '''
        wc = EquiJoin(self.conn, tpchSettings.key_lists, from_rels,
                      minimizer.global_min_instance_dict)

        wc.doJob(queries.Q23_1)

        self.assertEqual(len(wc.global_all_attribs), 4)  # per table 1 attrib, Q17 has 2 tables
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
        '''

        wc = Filter(self.conn, from_rels,
                    minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q23_1)
        print(filters)
        f = filters[0]

        self.assertEqual(len(filters), 1)
        self.assertEqual(f[0], 'region')
        self.assertEqual(f[1], 'r_name')
        self.assertEqual(f[2], 'equal')
        self.assertTrue('MIDDLE EAST' in f[3])

        self.conn.closeConnection()

    def test_join_graph_and_filter(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q18_test']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q18_test)
        self.assertTrue(check)

        wc = Filter(self.conn, from_rels, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q18_test)
        print(filters)
        self.assertEqual(len(filters), 3)
        filter_pred = ('part', 'p_size', '>=', 4, max_int_val)
        self.assertTrue(filter_pred in filters)

        ej = U2EquiJoin(self.conn, from_rels, wc.filter_predicates, wc, minimizer.global_min_instance_dict)
        equi = ej.doJob(queries.Q18_test)
        print(equi)
        self.assertEqual(len(equi), 1)
        self.assertFalse(len(ej.arithmetic_eq_predicates))
        self.assertEqual(len(equi.pending_predicates), 1)
        self.conn.closeConnection()

    def test_join_graph_and_filter1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q18_test1']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q18_test1)
        self.assertTrue(check)

        wc = Filter(self.conn, from_rels, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q18_test1)
        print(filters)
        self.assertEqual(len(filters), 4)
        filter_pred = ('part', 'p_size', '>=', 4, max_int_val)
        self.assertTrue(filter_pred in filters)
        cfilters = copy.deepcopy(filters)
        cfilters.remove(filter_pred)
        to_remove = [f for f in cfilters if f[2] == '=']
        cfilters = list(set(cfilters) - set(to_remove))
        print(cfilters)
        pred = cfilters[0]
        self.assertEqual(pred[0], 'part')
        self.assertEqual(pred[1], 'p_retailprice')
        self.assertEqual(pred[2], 'range')
        self.assertTrue(pred[3] > 800)
        self.assertTrue(pred[4] < 1000)

        ej = U2EquiJoin(self.conn, from_rels, wc.filter_predicates, wc, minimizer.global_min_instance_dict)
        equi = ej.doJob(queries.Q18_test1)
        print(equi)
        self.assertEqual(len(equi), 1)
        self.assertFalse(len(ej.arithmetic_eq_predicates))
        self.assertEqual(len(ej.pending_predicates), 2)

        self.conn.closeConnection()

    def test_join_graph_and_filterQ3(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q3)
        self.assertTrue(check)

        wc = Filter(self.conn, from_rels, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q3)
        print(filters)
        self.assertEqual(len(filters), 3)
        f = filters[0]
        self.assertEqual(f[0], 'customer')
        self.assertEqual(f[1], 'c_mktsegment')
        self.assertEqual(f[2], "equal")
        self.assertTrue('BUILDING' in f[3])

        f = filters[1]
        self.assertEqual(f[0], 'orders')
        self.assertEqual(f[1], 'o_orderdate')
        self.assertEqual(f[2], "<=")

        f = filters[2]
        self.assertEqual(f[0], 'lineitem')
        self.assertEqual(f[1], 'l_shipdate')
        self.assertEqual(f[2], ">=")

        self.conn.closeConnection()

    def test_filter_outer_join(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        self.conn.config.detect_oj = True
        query = "select n_name, r_comment from nation LEFT OUTER JOIN region " \
                "on n_regionkey = r_regionkey and r_name = 'AFRICA';"
        fc = FromClause(self.conn)
        check = fc.doJob(query)
        self.assertTrue(check)

        db_restorer = DbRestorer(self.conn, fc.core_relations)
        db_restorer.set_all_sizes(tpchSettings.all_size)
        check = db_restorer.doJob(None)
        self.assertTrue(check)

        minimizer = ViewMinimizer(self.conn, fc.core_relations, tpchSettings.all_size, False)
        check = minimizer.doJob(query)
        print(minimizer.global_min_instance_dict)
        self.assertTrue(check)

        wc = Filter(self.conn, fc.core_relations, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q3)
        print(filters)
        self.assertEqual(len(filters), 3)
        self.assertTrue(('region', 'r_name', 'equal', 'AFRICA', 'AFRICA') in filters)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
