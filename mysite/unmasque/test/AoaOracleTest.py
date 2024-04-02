import unittest

from ..refactored.equi_join import EquiJoin
from ..src.util.Oracle_connectionHelper import OracleConnectionHelper
from ..src.util.configParser import Config


class MyTestCase(unittest.TestCase):
    conn = OracleConnectionHelper(Config())

    def test_oracle_natural_join(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        from_rels = ['nation', 'region']
        global_min_instance_dict = {
            'nation': [('n_nationkey', 'numeric', 25), ('n_name', 'varchar', 30), ('r_regionkey', 'numeric', 25),
                       ('n_comment', 'varchar', 30)],
            'region': [('r_regionkey', 'numeric', 25), ('r_name', 'varchar', 30), ('r_comment', 'varchar', 30)]}
        key_lists = [[('part', 'p_partkey'), ('partsupp', 'ps_partkey'), ('lineitem', 'l_partkey')],
                     [('supplier', 's_suppkey'), ('partsupp', 'ps_suppkey'), ('lineitem', 'l_suppkey')],
                     [('supplier', 's_nationkey'), ('nation', 'N_NATIONKEY'), ('customer', 'c_nationkey')],
                     [('customer', 'c_custkey'), ('orders', 'o_custkey')],
                     [('orders', 'o_orderkey'), ('lineitem', 'l_orderkey')],
                     [('region', 'R_REGIONKEY'), ('nation', 'R_REGIONKEY')]]
        wc = EquiJoin(self.conn, key_lists, from_rels,
                      global_min_instance_dict)

        self.assertEqual(wc.global_attrib_types, [])
        self.assertEqual(wc.global_all_attribs, [])
        self.assertEqual(wc.global_d_plus_value, {})
        self.assertEqual(wc.global_attrib_max_length, {})

        wc.do_init()
        print(wc.global_attrib_types)
        check = wc.doJob("SELECT n_name FROM nation NATURAL JOIN region WHERE r_name = 'AFRICA'")
        self.assertTrue(check)
        self.conn.closeConnection()

    def test_test1(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(["ALTER SESSION SET CURRENT_SCHEMA = tpch"])
        res = self.conn.execute_sql_fetchall("""
            SELECT n_name 
            FROM nation NATURAL JOIN region 
            WHERE r_name = 'AFRICA'
        """)
        self.assertTrue(res)
        print(res)
        self.conn.closeConnection()

    def test_oracle_connection(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(["ALTER SESSION SET CURRENT_SCHEMA = tpch"])
        res, des = self.conn.execute_sql_fetchall("""
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
            FROM ALL_TAB_COLUMNS 
            WHERE TABLE_NAME = 'NATION' and OWNER = 'TPCH'
        """)
        self.assertTrue(res)

        print("Columns and Data Types for 'nation' table:")
        for row in res:
            print(row)
            self.assertTrue(row)

        res, des = self.conn.execute_sql_fetchall("""
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH 
                    FROM ALL_TAB_COLUMNS 
                    WHERE TABLE_NAME = 'REGION' and OWNER = 'TPCH'
                """)
        self.assertTrue(res)

        print("Columns and Data Types for 'region' table:")
        for row in res:
            print(row)
            self.assertTrue(row)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
