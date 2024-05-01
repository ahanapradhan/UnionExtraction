import unittest

from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory
from mysite.unmasque.src.util.QueryStringGenerator import QueryStringGenerator


class Sample:
    __param = 0

    @property
    def hello(self):
        raise NotImplemented

    @hello.setter
    def hello(self, value):
        print(value)
        self.__param = value

    def print(self):
        print(self.__param)


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

        for i in range(2 + 1, 10):
            print(i)

    def test_property(self):
        sample = Sample()
        sample.hello = 10
        sample.print()

    def test_remove_NE_string_q_gen(self):
        conn = ConnectionHelperFactory().createConnectionHelper()
        elf = ['partsupp', 'ps_comment', '<>', 'hello world regular mina dependencies']
        q_gen = QueryStringGenerator(conn)
        where_op = 'ps_suppkey = s_suppkey and s_nationkey = n_nationkey and ' \
                         'n_name = \'ARGENTINA\' and ps_comment <> \'dependencies\' ' \
                         'and ps_comment <> \'hello world regular mina dependencies\''
        q_gen._workingCopy.where_op = where_op
        q_gen._remove_exact_NE_string_predicate(elf)
        print(q_gen._workingCopy.where_op)
        self.assertEqual(q_gen._workingCopy.where_op, 'ps_suppkey = s_suppkey and '
                                         's_nationkey = n_nationkey and n_name = \'ARGENTINA\'')



if __name__ == '__main__':
    unittest.main()
