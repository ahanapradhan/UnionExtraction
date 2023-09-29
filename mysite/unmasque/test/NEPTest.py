import datetime
import unittest

from mysite.unmasque.refactored.cs2 import Cs2
from mysite.unmasque.refactored.nep import NEP
from mysite.unmasque.src.core.QueryStringGenerator import QueryStringGenerator
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_something(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem " \
                "Where l_shipdate  < '1994-01-01' " \
                "and l_quantity < 24 " \
                "and l_linenumber <> 4 and l_returnflag <> 'R' " \
                "Group By l_shipmode Limit 100; "

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate <= '1993-12-31' " \
              "and l_quantity < 24 " \
              "Group By l_shipmode Limit 100; "

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1, 1, 1), datetime.date(1993, 12, 31))]

        global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')
                               }

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = "l_quantity  <= 23.0 and l_shipdate  <= '1993-12-31'"

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                tpchSettings.key_lists,
                q_gen)

        check = o.doJob(query, Q_E)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("Where l_quantity  <= 23.0 and l_shipdate  <= '1993-12-31' and l_linenumber <> 4" in o.Q_E)

        self.conn.closeConnection()

    def test_Q6_mukul_thesis(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem Where l_shipdate >= " \
                "'1994-01-01' and l_quantity < 24 " \
                "and l_shipmode " \
                "not like '%AIR%' and l_shipdate <> '1995-01-03' Group By l_shipmode Limit 100;"

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate >= '1994-01-01' " \
              "and l_quantity <= 23.0 " \
              "Group By l_shipmode Limit 100; "

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1994, 1, 1), datetime.date(9999, 12, 31))]

        global_attrib_types = {('lineitem', 'l_orderkey', 'integer'), ('lineitem', 'l_partkey', 'integer'),
                               ('lineitem', 'l_suppkey', 'integer'), ('lineitem', 'l_linenumber', 'integer'),
                               ('lineitem', 'l_quantity', 'numeric'), ('lineitem', 'l_extendedprice', 'numeric'),
                               ('lineitem', 'l_discount', 'numeric'), ('lineitem', 'l_tax', 'numeric'),
                               ('lineitem', 'l_returnflag', 'character'), ('lineitem', 'l_linestatus', 'character'),
                               ('lineitem', 'l_shipdate', 'date'), ('lineitem', 'l_commitdate', 'date'),
                               ('lineitem', 'l_receiptdate', 'date'), ('lineitem', 'l_shipinstruct', 'character'),
                               ('lineitem', 'l_shipmode', 'character'),
                               ('lineitem', 'l_comment', 'character varying')
                               }

        global_all_attribs = [
            ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
             'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
             'l_shipmode', 'l_comment']]

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = 'l_shipmode'
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = ''
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = 'l_shipdate >= \'1994-01-01\' and l_quantity <= 23.0 '

        cs2 = Cs2(self.conn, tpchSettings.relations, core_rels, tpchSettings)
        cs2.take_backup()

        o = NEP(self.conn,
                core_rels,
                tpchSettings.all_size,
                tpchSettings.global_pk_dict,
                global_all_attribs,
                global_attrib_types,
                filters,
                tpchSettings.key_lists,
                q_gen)

        check = o.doJob(query, Q_E)
        self.assertTrue(check)
        print(o.Q_E)
        self.assertTrue("and l_shipmode NOT LIKE '%AIR%'" in o.Q_E)
        self.assertTrue("and l_shipdate <> '1994-01-03'" in o.Q_E)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
