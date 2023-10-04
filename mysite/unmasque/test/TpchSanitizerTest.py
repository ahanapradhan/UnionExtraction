import unittest

from mysite.unmasque.src.pipeline.abstract.TpchSanitizer import TpchSanitizer
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def test_for_table(self):
        sanitizer = TpchSanitizer(self.conn)
        self.conn.connectUsingParams()
        self.assertEqual('table', sanitizer.is_view_or_table("lineitem").lower())  # add assertion here
        self.assertEqual('table', sanitizer.is_view_or_table('lineitem').lower())  # add assertion here
        self.conn.closeConnection()

    def test_for_view(self):
        sanitizer = TpchSanitizer(self.conn)
        self.conn.connectUsingParams()
        self.conn.execute_sql(["create view nation1 as select * from nation;"])
        self.assertEqual('table', sanitizer.is_view_or_table("nation").lower())  # add assertion here
        self.assertEqual('table', sanitizer.is_view_or_table('nation').lower())  # add assertion here
        self.assertEqual('view', sanitizer.is_view_or_table("nation1").lower())  # add assertion here
        self.assertEqual('view', sanitizer.is_view_or_table('nation1').lower())  # add assertion here
        self.conn.execute_sql(["drop view if exists nation1;"])
        self.conn.closeConnection()

    def test_select_query(self):
        sanitizer = TpchSanitizer(self.conn)
        self.conn.connectUsingParams()
        q = sanitizer.select_query(["count(*)"], [])
        self.assertEqual(q, "Select count(*) From information_schema.tables WHERE table_schema = 'public' "
                            "and TABLE_CATALOG= 'tpch' ;")

        q = sanitizer.select_query(["SPLIT_PART(table_name, '_', 1) as original_name"], ["table_name like '%_restore'"])
        self.assertEqual(q, "Select SPLIT_PART(table_name, '_', 1) as original_name From information_schema.tables "
                            "WHERE table_schema = 'public' "
                            "and TABLE_CATALOG= 'tpch' and table_name like '%_restore' ;")
        q = sanitizer.select_query(["table_type"], ["table_name = 'lineitem'"])
        self.assertEqual(q, "Select table_type From information_schema.tables "
                            "WHERE table_schema = 'public' "
                            "and TABLE_CATALOG= 'tpch' and table_name = 'lineitem' ;")
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
