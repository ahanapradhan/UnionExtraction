import datetime
import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.nep import NEP
from mysite.unmasque.refactored.util.common_queries import get_restore_name, create_table_as_select_star_from
from mysite.unmasque.src.core.QueryStringGenerator import QueryStringGenerator
from mysite.unmasque.test import tpchSettings


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_something(self):
        self.conn.connectUsingParams()

        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem " \
                "Where l_shipdate  < '1994-01-01' " \
                "and l_quantity < 24 " \
                "and l_shipmode not like '%AIR%' " \
                "Group By l_shipmode Limit 100; "

        Q_E = "Select l_shipmode, sum(l_extendedprice) as revenue " \
              "From lineitem " \
              "Where l_shipdate <= '1994-12-30' " \
              "and l_quantity < 24 " \
              "Group By l_shipmode Limit 100; "

        self.conn.execute_sql(["create view test1 as " + Q_E, "drop view if exists test1;"])

        core_rels = ['lineitem']

        filters = [('lineitem', 'l_quantity', '<=', -2147483648.88, 23.0),
                   ('lineitem', 'l_shipdate', '<=', datetime.date(1, 1, 1), datetime.date(1993, 12, 30))]

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

        global_all_attribs = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                              'l_discount',
                              'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                              'l_shipinstruct',
                              'l_shipmode', 'l_comment']

        q_gen = QueryStringGenerator(self.conn)
        q_gen.from_op = 'lineitem'
        q_gen.group_by_op = ''
        q_gen.limit_op = '100'
        q_gen.method_call_count = 0
        q_gen.order_by_op = 'l_shipmode asc'
        q_gen.select_op = 'l_shipmode, Sum(l_extendedprice) as revenue'
        q_gen.where_op = "l_quantity  <= 23.0 and l_shipdate  <= '1993-12-30'"

        o = NEP(self.conn, core_rels, tpchSettings.all_size, tpchSettings.global_pk_dict,
                global_all_attribs, global_attrib_types, filters, tpchSettings.key_lists, q_gen)

        for tabname in core_rels:
            self.conn.execute_sql([create_table_as_select_star_from(get_restore_name(tabname), tabname)])

        check = o.doJob([query, Q_E])
        self.assertTrue(check)

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
