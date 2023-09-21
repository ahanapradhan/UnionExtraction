import unittest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def test_execute_query_on_db(self):
        app = Executable(self.conn)
        query = "select count(*) from public.region;"
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        result = app.doJob(query)
        self.assertTrue(result is not None)
        self.conn.closeConnection()
        self.assertEqual(app.method_call_count, 1)
        self.assertEqual(self.conn.conn, None)
        query = "select count(*) from public.nation;"
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        result = app.doJob(query)
        self.assertTrue(result is not None)
        self.conn.closeConnection()
        self.assertEqual(self.conn.conn, None)
        self.assertEqual(app.method_call_count, 2)


if __name__ == '__main__':
    unittest.main()
