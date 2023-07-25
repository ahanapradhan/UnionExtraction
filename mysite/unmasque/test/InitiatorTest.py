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

        # verify from "AutoJoin: Providing Freedom from Specifying Joins
        # University of Iowa Technical Report 04-03" Fig 5.
        # https://www.researchgate.net/publication/228738477_AutoJoin_Providing_Freedom_from_Specifying_Joins_University_of_Iowa_Technical_Report_04-03
        # combined key for lineitem and partsupp is missing in the result prduced by the code
        # analyze impact
        self.assertEqual(len(initor.global_key_lists), 6)
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
