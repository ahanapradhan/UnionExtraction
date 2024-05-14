import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory


class MyTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.conn = ConnectionHelperFactory().createConnectionHelper()
        self.conn.config.detect_oj = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        self.relations = ['customer', 'orders']

    def setUp(self):
        super().setUp()
        self.conn.connectUsingParams()
        for rel in self.relations:
            self.conn.execute_sql(["BEGIN;", f"alter table {rel} rename to {rel}_copy;",
                                   f"create table {rel} (like {rel}_copy);", "COMMIT;"])

        self.conn.execute_sql([
            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (5832099,149999, \'F\', 113195.49, \'1995, 1, 23\', \'1-URGENT\', \'Clerk#000000274\', 0, "
            f"\'ost slyly around the blithely bold requests.\');",

            f"Insert into customer(c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,"
            f"c_mktsegment,c_comment)"
            f"VALUES (2900, \'Customer#000002900\', \'xeicQEyv6I\', 9, \'19-292-999-1038\', "
            f"4794.87, \'HOUSEHOLD\', \' ironic packages. pending, regular deposits cajole blithely. carefully even "
            f"instructions engage stealthily carefull\');"
        ])
        self.conn.closeConnection()

    def tearDown(self):
        self.conn.connectUsingParams()
        for rel in self.relations:
            self.conn.execute_sql(["BEGIN;",
                                   f"drop table {rel};",
                                   f"alter table {rel}_copy rename to {rel};", "COMMIT;"])
        self.conn.closeConnection()
        super().tearDown()

    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
