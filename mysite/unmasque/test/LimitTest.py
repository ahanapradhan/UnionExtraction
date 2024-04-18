import datetime
import unittest

from mysite.unmasque.src.core.limit import Limit
from ..src.core.aoa import AlgebraicPredicate
from ..test.util import tpchSettings
from ..test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.conn.connectUsingParams()
        for rel in tpchSettings.relations:
            self.conn.execute_sql(["BEGIN;", f"alter table {rel} rename to {rel}_copy;",
                                   f"create table {rel} (like {rel}_copy);", "COMMIT;"])
        self.conn.closeConnection()

    def tearDown(self):
        self.conn.connectUsingParams()
        for rel in tpchSettings.relations:
            self.conn.execute_sql(["BEGIN;",
                                   f"drop table {rel};",
                                   f"alter table {rel}_copy rename to {rel};", "COMMIT;"])
        self.conn.closeConnection()
        super().tearDown()

    def test_something(self):
        self.conn.connectUsingParams()

        core_rels = ['orders', 'lineitem', 'partsupp']
        query = "Select l_quantity, l_shipinstruct From orders, lineitem, partsupp " \
                "Where ps_partkey = l_partkey " \
                "and ps_suppkey = l_suppkey " \
                "and o_orderkey = l_orderkey " \
                "and l_extendedprice <= 20000 " \
                "and o_totalprice <= 60000 " \
                "and ps_supplycost <= 500 " \
                "and l_linenumber = 1 " \
                "Order By l_orderkey Limit 10;"

        self.conn.execute_sql([
            f"Insert into lineitem(l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,"
            f"l_extendedprice,"
            f"l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,"
            f"l_shipinstruct,l_shipmode,l_comment)"
            f" VALUES (2999749,84936, 4937, 1, 1.00, 1920.93,0.01, 0.02, \'N\', \'O\', \'1998, 5, 6\',"
            f"\'1998, 5, 4\', \'1998, 5, 30\',\'COLLECT COD              \', \'AIR       \',"
            f"\'esias. furiously final foxes detec\');",

            f"Insert into orders(o_orderkey,o_custkey,o_orderstatus,o_totalprice,"
            f"o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment) "
            f"VALUES (2999749, 122899, \'O\', 56924.25, \'1998, 2, 8\', \'5-LOW\', \'Clerk#000000555\', 0, "
            f"\'luffily. slyly regular deposits will snooze. furiously pending asymptot\');",

            f"Insert into partsupp(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) "
            f"VALUES (84936, 4937, 8444, 26.97, \'riously final instructions. pinto beans cajole. "
            f"idly even packages haggle doggedly furiously regular \');"
        ])

        global_min_instance_dict = {'orders': [
            ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
             'o_clerk', 'o_shippriority', 'o_comment'),
            (2999749, 122899, 'O', 56924.25, datetime.date(1998, 2, 8), '5-LOW', 'Clerk#000000555', 0,
             'luffily. slyly regular deposits will snooze. furiously pending asymptot')],
            'lineitem': [('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                          'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                          'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
                         (2999749, 84936, 4937, 1, 1.00, 1920.93,
                          0.01, 0.02, 'N', 'O', datetime.date(1998, 5, 6),
                          datetime.date(1998, 5, 4), datetime.date(1998, 5, 30),
                          'COLLECT COD              ', 'AIR       ',
                          'esias. furiously final foxes detec')],
            'partsupp': [('ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'),
                         (84936, 4937, 8444, 26.97,
                          'riously final instructions. pinto beans cajole. idly even packages haggle doggedly '
                          'furiously regular ')]}

        aoa = AlgebraicPredicate(self.conn, core_rels, pending_predicates, filter_extractor, global_min_instance_dict)
        aoa.mock = True
        check = aoa.doJob(query)
        self.assertTrue(check)
        print(aoa.where_clause)
        lm = Limit(self.conn, [], aoa.pipeline_delivery)
        lm.doJob(query)
        self.assertEqual(10, lm.limit)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
