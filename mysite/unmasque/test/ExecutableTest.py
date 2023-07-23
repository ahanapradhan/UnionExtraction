import unittest

from mysite.unmasque.u_global import executable
from mysite.unmasque.u_global.ConnectionHelper import ConnectionHelper


class MyTestCase(unittest.TestCase):
    def test_execute_query_on_db(self):
        query = "select count(*) from public.region;"
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        result = executable.getExecOutput(conn, query)
        self.assertTrue(result is not None)
        conn.closeConnection()
        self.assertEqual(conn.conn, None)


if __name__ == '__main__':
    unittest.main()
