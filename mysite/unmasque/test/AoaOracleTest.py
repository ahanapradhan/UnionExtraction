import unittest

import pytest

from ..src.core.aoa import AlgebraicPredicate
from ..src.util.Oracle_connectionHelper import OracleConnectionHelper
from ..src.util.configParser import Config


class MyTestCase(unittest.TestCase):
    conn = OracleConnectionHelper(Config())
    schema = 'tpch'
    tables = ['lineitem', 'orders', 'customer', 'nation', 'region']
    output = "share.sql"
    ddl = "ddl.sql"

    def test_print_ddls(self):
        self.conn.connectUsingParams()
        self.set_schema()
        with open(self.ddl, 'a') as f:
            for table_name in self.tables:
                res = self.conn.execute_sql_fetchone_0(
                    f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name.upper()}', '{self.conn.config.schema.upper()}') FROM DUAL")
                f.write(str(res))
                f.write("\n\n\n")
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_oracle_connection(self):
        self.conn.connectUsingParams()
        self.set_schema()
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

    def set_schema(self):
        self.conn.execute_sql([f"ALTER SESSION SET CURRENT_SCHEMA = {self.conn.config.schema}"])

    def test_oracle_connection_aoa(self):
        nationkey_filter = 13
        query = f'SELECT n_name, r_name FROM {self.conn.config.schema}.nation NATURAL JOIN {self.conn.config.schema}.region WHERE nationkey = {nationkey_filter}'
        self.conn.connectUsingParams()
        self.set_schema()
        res = self.conn.execute_sql_fetchall(query)
        self.assertTrue(res)
        core_rels = ['nation', 'region']
        global_min_instance_dict = {'nation': [
            ('nationkey', 'n_name', 'regionkey', 'n_comment'),
            (nationkey_filter, 'ALGERIA', 1, 'embark quickly. bold foxes adapt slyly')],
            'region': [('regionkey', 'r_name', 'r_comment'),
                       (1, 'AFRICA', 'nag efully about the slyly bold instructions. quickly regular pinto beans wake '
                                     'blithely')]}
        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.assertTrue(aoa.where_clause)
        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def put_to_output(self, aoa, query):
        with open(self.output, 'a') as f:
            f.write(query)
            f.write("\n")
            f.write("\n/* Extracted WHERE Clause: */\n")
            f.write(aoa.where_clause)
            f.write("\n\n\n")

    def test_customer_orders_nation(self):
        global_min_instance_dict = {'orders': [
            ('orderkey', 'custkey', 'o_orderstatus', 'o_totalprice', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (55620, 2900, 'O', 2590.94, '4-NOT SPECIFIED', 'Clerk#000000311', 0,
             'usly. regular, regul')], 'customer': [
            ('custkey', 'c_name', 'c_address', 'nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),
            (2900, 'Customer#000002900', 'xeicQEyv6I', 9, '19-292-999-1038', 4794.87, 'HOUSEHOLD',
             ' ironic packages. pending, regular deposits cajole blithely. carefully even '
             'instructions engage stealthily carefull')], 'nation': [
            ('nationkey', 'n_name', 'regionkey', 'n_comment'),
            (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. '
                                'carefully ironic hockey players sleep blithely. carefull')]}
        self.conn.connectUsingParams()
        self.set_schema()

        relations = ['customer', 'nation', 'orders']

        query = f"SELECT n_name as name, " \
                f"c_acctbal as account_balance " \
                f"FROM {self.conn.config.schema}.orders NATURAL JOIN {self.conn.config.schema}.customer " \
                f"NATURAL JOIN {self.conn.config.schema}.nation " \
                f"WHERE o_totalprice <= c_acctbal"

        aoa = AlgebraicPredicate(self.conn, relations, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def test_orders_lineitem_aoa(self):
        self.conn.connectUsingParams()
        query = f"Select o_orderstatus, l_shipmode From {self.conn.config.schema}.orders natural join {self.conn.config.schema}.lineitem " \
                "Where l_linenumber >= 6 "

        self.set_schema()
        res = self.conn.execute_sql_fetchall(query)
        self.assertTrue(res)

        core_rels = ['orders', 'lineitem']

        global_min_instance_dict = {'orders': [
            ('orderkey', 'custkey', 'o_orderstatus', 'o_totalprice', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999908, 23014, 'F', 168982.73, '5-LOW', 'Clerk#000000061', 0,
             'ost slyly around the blithely bold requests.')],
            'lineitem': [('orderkey', 'partkey', 'suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipinstruct', 'l_shipmode',
                          'l_comment'),
                         (2999908, 2997, 4248, 6, 19.00, 36099.81, 0.04, 0.08, 'R', 'F',
                          'NONE                     ', 'AIR       ',
                          're. unusual frets after the sl')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def test_lineitem_orders_aoa(self):
        self.conn.connectUsingParams()
        query = f"Select o_orderstatus, l_shipmode From {self.conn.config.schema}.lineitem natural join {self.conn.config.schema}.orders " \
                "Where l_linenumber >= 4 "

        self.set_schema()
        res = self.conn.execute_sql_fetchall(query)
        self.assertTrue(res)

        core_rels = ['orders', 'lineitem']

        global_min_instance_dict = {'orders': [
            ('orderkey', 'custkey', 'o_orderstatus', 'o_totalprice', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999908, 23014, 'F', 168982.73, '5-LOW', 'Clerk#000000061', 0,
             'ost slyly around the blithely bold requests.')],
            'lineitem': [('orderkey', 'partkey', 'suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipinstruct', 'l_shipmode',
                          'l_comment'),
                         (2999908, 2997, 4248, 6, 19.00, 36099.81, 0.04, 0.08, 'R', 'F',
                          'NONE                     ', 'AIR       ',
                          're. unusual frets after the sl')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    # @pytest.mark.skip
    def test_lineitem_orders_customer_nation_aoa(self):
        self.conn.connectUsingParams()
        query = f"Select o_orderstatus, l_shipmode From {self.conn.config.schema}.lineitem " \
                 f"natural join {self.conn.config.schema}.orders " \
                f"natural join {self.conn.config.schema}.customer "\
                f"natural join {self.conn.config.schema}.nation " \
                "Where l_linenumber >= 4 and n_name LIKE 'IND%' " \
                 " and l_extendedprice < o_totalprice"

        self.set_schema()
        res = self.conn.execute_sql_fetchall(query)
        self.assertTrue(res)

        core_rels = ['orders', 'lineitem', 'customer', 'nation']

        global_min_instance_dict = {'orders': [
            ('orderkey', 'custkey', 'o_orderstatus', 'o_totalprice', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999908, 23014, 'F', 168982.73, '5-LOW', 'Clerk#000000061', 0,
             'ost slyly around the blithely bold requests.')],
            'lineitem': [('orderkey', 'partkey', 'suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipinstruct', 'l_shipmode',
                          'l_comment'),
                         (2999908, 2997, 4248, 6, 19.00, 36099.81, 0.04, 0.08, 'R', 'F',
                          'NONE                     ', 'AIR       ',
                          're. unusual frets after the sl')],
            'customer': [('custkey', 'c_name', 'c_address', 'nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
             'c_comment'),(23014, 'Customer#000002900', 'xeicQEyv6I', 9, '19-292-999-1038', 4794.87, 'HOUSEHOLD',
             ' ironic packages. pending, regular deposits cajole blithely. carefully even '
             'instructions engage stealthily carefull')],
            'nation': [('nationkey', 'n_name', 'regionkey', 'n_comment'),
            (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. '
                                'carefully ironic hockey players sleep blithely. carefull')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        self.conn.closeConnection()
        self.put_to_output(aoa, query)


if __name__ == '__main__':
    unittest.main()
