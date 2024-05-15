import unittest

from mysite.unmasque.src.core.limit import Limit
from mysite.unmasque.src.core.orderby_clause import OrderBy
from mysite.unmasque.src.util.QueryStringGenerator import QueryStringGenerator
from mysite.unmasque.src.core.aggregation import Aggregation
from mysite.unmasque.src.core.groupby_clause import GroupBy
from mysite.unmasque.src.core.projection import Projection
from ..src.core.aoa import AlgebraicPredicate
from ..src.util.Oracle_connectionHelper import OracleConnectionHelper
from ..src.util.configParser import Config


class MyTestCase(unittest.TestCase):
    conn = OracleConnectionHelper(Config())
    schema = 'tpch'
    tables = ['lineitem', 'orders', 'customer', 'nation', 'region']
    output = "share.sql"
    ddl = "ddl.sql"

    def setUp(self):
        self.conn.connectUsingParams()
        # self.drop_tables()
        # self.create_tables()
        # self.insert_dmin_data()
        self.conn.closeConnection()

    def tearDown(self):
        self.conn.connectUsingParams()
        # self.drop_tables()
        # self.create_tables()
        # self.insert_dmin_data()
        self.conn.closeConnection()

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

    def create_tables(self):
        self.conn.execute_sql([
            """
            create table TPCH.region( regionkey number(10) not null, r_name varchar2(25) not null, r_comment varchar2(152) not null )
            """,

            """
            create table  TPCH.nation( nationkey number(10) not null, n_name varchar2(25) not null, regionkey number(10) not null, n_comment varchar2(152) not null)
             """,

            """
            create table tpch.orders(orderkey number(10) not null, custkey number(10) not null, o_orderstatus char(1) not null,
            o_totalprice number not null,
            o_orderpriority varchar2(15) not null,
            o_clerk varchar2(15) not null,
            o_shippriority integer not null,
            o_comment varchar2(79) not NULL)
        """,
            """
        create table tpch.customer(custkey number(10),
            c_name varchar2(25),
            c_address varchar2(40),
            nationkey number(10),
            c_phone varchar2(15),
            c_acctbal number,
            c_mktsegment varchar2(10),
            c_comment varchar2(117))
             """,

            """
            create table tpch.lineitem( orderkey number(10),
            partkey number(10), suppkey number(10),
            l_linenumber number(38),
            l_quantity number, l_extendedprice number,
            l_discount number,
            l_tax number,
            l_returnflag char(1),
            l_linestatus char(1),
            l_shipinstruct varchar2(25),
            l_shipmode varchar2(10),
            l_comment varchar2(44) ) 
        """])

    def insert_dmin_data(self):
        self.conn.execute_sql([
            """
        INSERT INTO tpch.orders(orderkey, custkey, o_orderstatus, o_totalprice, o_orderpriority, o_clerk, o_shippriority, o_comment)
            VALUES(2999908, 23014, 'F', 168982.73, '5-LOW', 'Clerk#000000061', 0,
                   'ost slyly around the blithely bold requests.')
                    """,

            """
            INSERT INTO tpch.lineitem(orderkey, partkey, suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
                          l_returnflag, l_linestatus, l_shipinstruct, l_shipmode, l_comment)
            VALUES(2999908, 2997, 4248, 6, 19.00, 36099.81, 0.04, 0.08, 'R', 'F','NONE', 'AIR',
                   're. unusual frets after the sl')
        """,

            """
        INSERT INTO tpch.customer(custkey, c_name, c_address, nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
            VALUES(23014, 'Customer1', 'Address1', 1, '1234567890', 1000.00, 'Segment1', 'This is a comment.')
             """,

            """
            INSERT INTO TPCH.nation(nationkey, n_name, regionkey, n_comment)
            VALUES(1, 'ALGERIA', 13, 'embark quickly. bold foxes adapt slyly')
        """,

            """
        INSERT INTO TPCH.region(regionkey, r_name, r_comment)
            VALUES(1, 'AFRICA', 'nag efully about the slyly bold instructions. quickly regular pinto beans wake blithely')
             """,
        ])

    def drop_tables(self):
        for table in self.tables:
            self.conn.execute_sql([self.conn.queries.drop_table(table)])

    def set_schema(self):
        self.conn.execute_sql([f"ALTER SESSION SET CURRENT_SCHEMA = {self.conn.config.schema}"])

    def test_oracle_connection_aoa(self):
        nationkey_filter = 13
        query = f'SELECT count(n_name) as total, r_name FROM {self.conn.config.schema}.nation NATURAL JOIN {self.conn.config.schema}.region WHERE nationkey = {nationkey_filter} group by r_name order by total, r_name desc FETCH FIRST 15 ROWS ONLY'
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
        aoa = self.run_pipeline(global_min_instance_dict, query, core_rels)
        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def put_to_output(self, aoa, query):
        print(query)
        with open(self.output, 'a') as f:
            f.write(query)
            f.write("\n")
            f.write("\n/* Extracted Query: */\n")
            f.write(aoa)
            f.write("\n\n\n")
        print(aoa)

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

        query = f"SELECT n_name AS name, " \
                f"SUM(c_acctbal) AS account_balance " \
                f"FROM {self.conn.config.schema}.orders NATURAL JOIN {self.conn.config.schema}.customer " \
                f"NATURAL JOIN {self.conn.config.schema}.nation " \
                f"WHERE o_totalprice <= 70000 group by n_name order by account_balance desc FETCH FIRST 5 ROW ONLY"

        aoa = self.run_pipeline(global_min_instance_dict, query, relations)

        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def test_customer_orders_nation_aoa(self):
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

        query = f"SELECT n_name as name, AVG(c_acctbal) as avg_balance FROM " \
                f"{self.conn.config.schema}.orders NATURAL JOIN {self.conn.config.schema}.customer " \
                f"NATURAL JOIN {self.conn.config.schema}.nation WHERE o_totalprice <= c_acctbal GROUP BY n_name " \
                f"ORDER BY avg_balance desc, n_name asc FETCH FIRST 23 ROWS ONLY"

        aoa = self.run_pipeline(global_min_instance_dict, query, relations)

        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def run_pipeline(self, global_min_instance_dict, query, relations):
        print(global_min_instance_dict)
        aoa = AlgebraicPredicate(self.conn, relations, pending_predicates, filter_extractor, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        pj = Projection(self.conn, aoa.nextPipelineCtx)
        pj.mock = True
        check = pj.doJob(query)
        self.assertTrue(check)
        print(pj.projected_attribs)
        gb = GroupBy(self.conn, aoa.nextPipelineCtx, pj.projected_attribs)
        check = gb.doJob(query)
        self.assertTrue(check)
        agg = Aggregation(self.conn, pj.projected_attribs, gb.has_groupby, gb.group_by_attrib,
                          pj.dependencies, pj.solution, pj.param_list, aoa.nextPipelineCtx)
        agg.doJob(query)
        self.assertTrue(agg.done)
        ob = OrderBy(self.conn, pj.projected_attribs, pj.projection_names, pj.dependencies,
                     agg.global_aggregated_attributes, aoa.nextPipelineCtx)
        ob.doJob(query)
        self.assertTrue(ob.done)
        lm = Limit(self.conn, gb.group_by_attrib, aoa.nextPipelineCtx)
        lm.doJob(query)
        self.assertTrue(lm.done)
        eq = QueryStringGenerator(self.conn).generate_query_string(relations, pj, agg, ob, lm, aoa)
        return eq

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

        aoa = self.run_pipeline(global_min_instance_dict, query, core_rels)

        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    def test_lineitem_orders_aoa(self):
        self.conn.connectUsingParams()
        query = f"Select o_orderstatus, l_shipmode From {self.conn.config.schema}.lineitem natural join {self.conn.config.schema}.orders " \
                "Where l_linenumber >= 4 and o_clerk LIKE 'Clerk%61' "

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

        aoa = self.run_pipeline(global_min_instance_dict, query, core_rels)

        self.conn.closeConnection()
        self.put_to_output(aoa, query)

    # @pytest.mark.skip
    def test_lineitem_orders_customer_nation_aoa(self):
        self.conn.connectUsingParams()
        query = f"Select o_orderstatus, l_shipmode From {self.conn.config.schema}.lineitem " \
                f"natural join {self.conn.config.schema}.orders " \
                f"natural join {self.conn.config.schema}.customer " \
                f"natural join {self.conn.config.schema}.nation " \
                "Where l_linenumber >= 4 and n_name LIKE 'IND%' " \
                " and l_extendedprice < 60000"

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
                          'c_comment'),
                         (23014, 'Customer#000002900', 'xeicQEyv6I', 9, '19-292-999-1038', 4794.87, 'HOUSEHOLD',
                          ' ironic packages. pending, regular deposits cajole blithely. carefully even '
                          'instructions engage stealthily carefull')],
            'nation': [('nationkey', 'n_name', 'regionkey', 'n_comment'),
                       (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. '
                                           'carefully ironic hockey players sleep blithely. carefull')]}

        aoa = self.run_pipeline(global_min_instance_dict, query, core_rels)

        self.conn.closeConnection()
        self.put_to_output(aoa, query)


if __name__ == '__main__':
    unittest.main()
