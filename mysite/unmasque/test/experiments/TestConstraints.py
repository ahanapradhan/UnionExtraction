import unittest

from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory


class MyTestCase(unittest.TestCase):

    def test_check(self):
        self.conn = ConnectionHelperFactory().createConnectionHelper()
        self.conn.connectUsingParams()

        tab = 'nation'
        pk = 'n_nationkey'
        pks, _ = self.conn.execute_sql_fetchall(self.conn.queries.select_attribs_from_relation([pk], tab))
        for key in pks:
            print(f"update {tab} set {pk} = {key[0]};")
        self.conn.closeConnection()

        for i in range(2 + 1 , 10):
            print(i)


if __name__ == '__main__':
    unittest.main()
