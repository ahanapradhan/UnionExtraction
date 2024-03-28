import unittest

from ..src.core.aoa import AlgebraicPredicate
from ..src.util.ConnectionHelper import ConnectionHelper


class MyTestCase(unittest.TestCase):
    def test_oracle_connection(self):
        conn = ConnectionHelper()
        conn.config.database = "oracle"
        conn.database = "oracle"
        conn.config.password = "postgres"
        conn.config.host = "HP-Z4-G4-Workstation"
        conn.config.port = "1539"
        conn.config.user = "TPCH"
        conn.connectUsingParams()
        res = conn.execute_sql_fetchall("SELECT n_name, r_name FROM tpch.nation, tpch.region WHERE n_nationkey = 1")
        self.assertTrue(res)
        print(res)
        query = 'SELECT n_name, r_name FROM tpch.nation, tpch.region WHERE n_nationkey = 1'
        core_rels = ['tpch.nation', 'tpch.region']
        global_min_instance_dict = {'tpch.nation': [
            ('n_nationkey', 'n_name', 'r_regionkey', 'n_comment'),
            (1, 'ALGERIA', 1, 'embark quickly. bold foxes adapt slyly')],
            'tpch.region': [('r_regionkey', 'r_name', 'r_comment'),
                       (1, 'AFRICA', 'nag efully about the slyly bold instructions. quickly regular pinto beans wake '
                                     'blithely')]}
        aoa = AlgebraicPredicate(conn, core_rels, global_min_instance_dict)
        aoa.filter_extractor.set_col_data_getter_method(self.feed_dummy_col_data)
        aoa.mock = True
        check = aoa.doJob(query)
        print(aoa.join_graph)
        self.assertTrue(check)
        print(aoa.where_clause)
        conn.closeConnection()

    def feed_dummy_col_data(self, tabname):
        if tabname == 'tpch.nation':
            return [('n_nationkey', 'numeric', 25), ('n_name', 'varchar', 30), ('r_regionkey', 'numeric', 25), ('n_comment', 'varchar', 30)]
        elif tabname == 'tpch.region':
            return [('r_regionkey', 'numeric', 25), ('r_name', 'varchar', 30), ('r_comment', 'varchar', 30)]

if __name__ == '__main__':
    unittest.main()
