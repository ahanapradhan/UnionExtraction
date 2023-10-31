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

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
