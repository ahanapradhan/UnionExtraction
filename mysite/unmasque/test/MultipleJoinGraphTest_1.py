import unittest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.core.multiple_equi_joins import MultipleEquiJoin
from mysite.unmasque.src.core.n_minimizer import NMinimizer
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    tab_customer = "customer"
    tab_nation = "nation"
    tab_supplier = "supplier"
    tab_orders = "orders"
    tab_lineitem = "lineitem"

    def test_for_same_joins_two_graphs(self):
        self.conn.connectUsingParams()
        relations = [self.tab_nation, self.tab_customer]
        n_minimizer = NMinimizer(self.conn, relations, tpchSettings.all_size)
        n_minimizer.mock = True

        query = "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'BRAZIL' INTERSECT " \
                "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'ARGENTINA';"
        print(query)
        check = n_minimizer.doJob(query)
        self.assertTrue(check)
        self.assertEqual(2, n_minimizer.core_sizes[self.tab_nation])
        self.assertEqual(2, n_minimizer.core_sizes[self.tab_customer])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        n_minimizer.see_d_min()

        equi_join = MultipleEquiJoin(self.conn, tpchSettings.key_lists, relations, n_minimizer.global_min_instance_dict)

        check = equi_join.doJob(query)
        self.assertTrue(check)
        print(equi_join.global_join_graph)
        self.assertEqual(2, equi_join.join_key_subquery_dict['c_nationkey'])
        self.assertEqual(2, equi_join.join_key_subquery_dict['n_nationkey'])

        print(equi_join.global_all_join_graphs)
        self.assertEqual(2, len(equi_join.global_all_join_graphs))
        for join_graph in equi_join.global_all_join_graphs:
            self.assertEqual(1, len(join_graph))
            edge = join_graph[0]
            self.assertTrue('c_nationkey' in edge)
            self.assertTrue('n_nationkey' in edge)

        self.conn.closeConnection()

    def test_adonis_case_1(self):
        self.conn.connectUsingParams()
        relations = [self.tab_nation, self.tab_customer, self.tab_orders]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "select c_mktsegment as segment from customer,nation " \
                "where c_acctbal < 3000 and c_nationkey = n_nationkey and n_name = 'BRAZIL' " \
                "intersect " \
                "select c_mktsegment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey=n_nationkey and c_custkey = o_custkey " \
                "and n_name = 'ARGENTINA';"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(2, nm.core_sizes[self.tab_customer])
        self.assertEqual(1, nm.core_sizes[self.tab_orders])
        self.assertEqual(2, nm.core_sizes[self.tab_nation])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        print(res)
        nm.see_d_min()

        equi_join = MultipleEquiJoin(self.conn, tpchSettings.key_lists, relations, nm.global_min_instance_dict)

        check = equi_join.doJob(query)
        self.assertTrue(check)
        print(equi_join.global_join_graph)
        self.assertEqual(2, equi_join.join_key_subquery_dict['c_nationkey'])
        self.assertEqual(2, equi_join.join_key_subquery_dict['n_nationkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['c_custkey'])
        self.assertEqual(1, equi_join.join_key_subquery_dict['o_custkey'])

        print(equi_join.global_all_join_graphs)
        self.assertEqual(2, len(equi_join.global_all_join_graphs))
        for join_graph in equi_join.global_all_join_graphs:
            self.assertTrue(len(join_graph) == 1 or len(join_graph) == 2)

            if len(join_graph) == 1:
                edge = join_graph[0]
                self.assertTrue('c_nationkey' in edge)
                self.assertTrue('n_nationkey' in edge)
            else:
                edge = join_graph[0]
                self.assertTrue('c_nationkey' in edge)
                self.assertTrue('n_nationkey' in edge)
                edge = join_graph[1]
                self.assertTrue('c_custkey' in edge)
                self.assertTrue('o_custkey' in edge)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
