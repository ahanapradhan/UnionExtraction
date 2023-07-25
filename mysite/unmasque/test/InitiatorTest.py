import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.initialization import Initiator


class MyTestCase(unittest.TestCase):
    def test_init(self):
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        initor = Initiator(conn)
        initor.doJob()
        self.assertEqual(len(initor.global_index_dict), 8)

        print(len(initor.global_key_lists))
        for key_list in initor.global_key_lists:
            print(key_list)

        # verify correctness from: https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-pkeys.sql
        self.assertEqual(len(initor.global_pk_dict), 8)
        self.assertEqual(initor.global_pk_dict['part'], 'p_partkey')
        self.assertEqual(initor.global_pk_dict['supplier'], 's_suppkey')
        self.assertEqual(initor.global_pk_dict['partsupp'], 'ps_partkey,ps_suppkey')
        self.assertEqual(initor.global_pk_dict['customer'], 'c_custkey')
        self.assertEqual(initor.global_pk_dict['orders'], 'o_orderkey')
        self.assertEqual(initor.global_pk_dict['nation'], 'n_nationkey')
        self.assertEqual(initor.global_pk_dict['region'], 'r_regionkey')
        self.assertEqual(initor.global_pk_dict['lineitem'], 'l_orderkey,l_linenumber')









if __name__ == '__main__':
    unittest.main()
