import unittest

from ..src.core.factory.PipeLineFactory import PipeLineFactory
from ..src.util.ConnectionFactory import ConnectionHelperFactory


def get_inner_join_query():
    return f"SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name" \
           f" from orders INNER JOIN customer" \
           f" on c_custkey = o_custkey and o_orderstatus = 'F' " \
           f"group by o_custkey, o_clerk, c_name order by key;"


class MyTestCase(unittest.TestCase):

    def _do_init(self, limit=100):
        self.conn = ConnectionHelperFactory().createConnectionHelper()
        self.conn.config.detect_oj = True
        self.conn.config.limit_limit = limit
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        self.relations = ['customer', 'orders']
        self.join_type = "FULL"
        self.conn.connectUsingParams()
        for rel in self.relations:
            self.conn.execute_sql(["BEGIN;",
                                   f"drop table if exists {rel}_copy cascade;",
                                   f"alter table {rel} rename to {rel}_copy;",
                                   f"create table {rel} (like {rel}_copy);", "COMMIT;"])

        self.conn.execute_sql([
            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (5832099,149999, \'F\', 113195.49, \'1995, 1, 23\', \'1-URGENT\', \'Clerk#000000274\', 0, "
            f"\'ost slyly around the blithely bold requests.\');",

            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (149999, \'Customer#000002900\', \'xeicQEyv6I\', 1, \'19-292-999-1038\', "
            f"6104.03, \'AUTOMOBILE\', \' ironic packages. pending, regular deposits cajole blithely. carefully even "
            f"instructions engage stealthily carefull\');"
        ])
        self.conn.closeConnection()

    def _do_exit(self):
        self.conn.connectUsingParams()
        for rel in self.relations:
            self.conn.execute_sql(["BEGIN;",
                                   f"drop table {rel};",
                                   f"alter table {rel}_copy rename to {rel};", "COMMIT;"])
        self.conn.closeConnection()

    def do_testJob(self, query, limit=102):
        self._do_init(limit)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        self._do_exit()
        return eq

    def test_full_outer_join(self):
        self.join_type = "FULL"
        self.do_testJob(self.get_outer_join_query())

    def test_left_outer_join(self):
        self.join_type = "LEFT"
        self.do_testJob(self.get_outer_join_query())

    def test_right_outer_join(self):
        self.join_type = "RIGHT"
        limit = 500
        self.do_testJob(self.get_outer_join_query(135), limit)

    def test_right_outer_join_higher_limit(self):
        self.join_type = "RIGHT"
        limit = 1500
        self.do_testJob(self.get_outer_join_query(1337), limit)

    def get_outer_join_query(self, limit=100):
        return f"SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name" \
               f" from orders {self.join_type} OUTER JOIN customer" \
               f" on c_custkey = o_custkey and o_orderstatus = 'F' " \
               f"group by o_custkey, o_clerk, c_name order by key LIMIT {limit};"

    def test_limit_upto_100(self):
        self.join_type = "FULL"
        for lim in [10, 25, 96, 45, 8, 98]:
            self.do_testJob(self.get_outer_join_query(lim))

    def test_limit_more_than_default(self):
        self.join_type = "FULL"
        for lim in [597, 1233]:
            self.do_testJob(self.get_outer_join_query(lim), lim + 2)

    def test_inner_join(self):
        eq = self.do_testJob(get_inner_join_query())
        self.assertEqual(eq.count("INNER"), 0)


if __name__ == '__main__':
    unittest.main()
