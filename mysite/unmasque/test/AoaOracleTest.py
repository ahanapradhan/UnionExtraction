import unittest

from ..src.util.Oracle_connectionHelper import OracleConnectionHelper
from ..src.core.aoa import AlgebraicPredicate

from ..src.util.configParser import Config


class MyTestCase(unittest.TestCase):
    conn = OracleConnectionHelper(Config())

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

        query = 'SELECT n_name, r_name FROM tpch.nation NATURAL JOIN tpch.region WHERE n_nationkey = 13'
        res, _ = self.conn.execute_sql_fetchall(query)
        print(res)
        self.assertTrue(res)
        self.conn.closeConnection()

    def test_oracle_connection_aoa(self):
        query = 'SELECT n_name, r_name FROM tpch.nation NATURAL JOIN tpch.region WHERE n_nationkey = 13'
        self.conn.connectUsingParams()
        self.conn.execute_sql(["ALTER SESSION SET CURRENT_SCHEMA = tpch"])
        res = self.conn.execute_sql_fetchall(query)
        self.assertTrue(res)
        # print(res)
        core_rels = ['nation', 'region']
        global_min_instance_dict = {'nation': [
            ('N_NATIONKEY', 'N_NAME', 'R_REGIONKEY', 'N_COMMENT'),
            (13, 'ALGERIA', 1, 'embark quickly. bold foxes adapt slyly')],
            'region': [('R_REGIONKEY', 'R_NAME', 'R_COMMENT'),
                       (1, 'AFRICA', 'nag efully about the slyly bold instructions. quickly regular pinto beans wake '
                                     'blithely')]}
        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        # print(aoa.join_graph)
        self.assertTrue(check)
        print(aoa.where_clause)
        # print(aoa.filter_predicates)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
