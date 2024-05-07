import unittest

from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory
from mysite.unmasque.src.util.QueryStringGenerator import QueryStringGenerator, QueryDetails


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

    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.global_attrib_types_dict = {}
        self.global_attrib_types = None
        self.conn = ConnectionHelperFactory().createConnectionHelper()

    def do_init(self):
        for entry in self.global_attrib_types:
            self.global_attrib_types_dict[(entry[0], entry[1])] = entry[2]

    def get_datatype(self, tab_attrib):
        if any(x in self.global_attrib_types_dict[tab_attrib] for x in ['int', 'integer', 'number']):
            return 'int'
        elif 'date' in self.global_attrib_types_dict[tab_attrib]:
            return 'date'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['text', 'char', 'varbit']):
            return 'str'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['numeric', 'float']):
            return 'numeric'
        else:
            raise ValueError

    def test_check(self):
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
        elf = ['partsupp', 'ps_comment', '<>', 'hello world regular mina dependencies']
        q_gen = QueryStringGenerator(self.conn)
        where_op = 'ps_suppkey = s_suppkey and s_nationkey = n_nationkey and ' \
                   'n_name = \'ARGENTINA\' and ps_comment <> \'dependencies\' ' \
                   'and ps_comment <> \'hello world regular mina dependencies\''
        q_gen.where_op = where_op
        q_gen._remove_exact_NE_string_predicate(elf)
        print(q_gen.where_op)
        self.assertEqual(q_gen.where_op, 'ps_suppkey = s_suppkey and '
                                         's_nationkey = n_nationkey and n_name = \'ARGENTINA\'')

    def test_qgen(self):
        q_gen = QueryStringGenerator(self.conn)
        q_gen.where_op = "Hello"
        self.assertEqual("Hello", q_gen.where_op)
        self.assertEqual("Hello", q_gen._workingCopy.where_op)

    def test_qgen_nestedQuery(self):
        self.conn.connectUsingParams()
        q_gen = QueryStringGenerator(self.conn)
        testDetails = QueryDetails()
        testDetails.core_relations = ['customer', 'orders']

        testDetails.eq_join_predicates = [[('customer', 'c_custkey'), ('orders', 'o_custkey')]]
        testDetails.join_graph = [[('c_custkey', 'o_custkey')]]
        testDetails.filter_in_predicates = []
        testDetails.filter_predicates = [('customer', 'c_mktsegment', 'equal', 'BUILDING', 'BUILDING'),
                                         ('orders', 'o_totalprice', "<=", -2147483648.88, 120000)]
        testDetails.aoa_less_thans = []
        testDetails.aoa_predicates = []
        testDetails.join_edges = ['customer.c_custkey = orders.o_custkey']

        testDetails.projection_names = ['c_name', 'c_acctbal']
        testDetails.global_projected_attributes = ['c_name', 'c_acctbal']
        testDetails.global_groupby_attributes = []
        testDetails.global_aggregated_attributes = [['c_name', ''], ['c_acctbal', '']]
        testDetails.global_key_attributes = ['c_custkey', 'o_custkey']

        self.global_attrib_types = [('customer', 'c_custkey', 'integer'),
                                    ('customer', 'c_name', 'character varying'),
                                    ('customer', 'c_address', 'character varying'),
                                    ('customer', 'c_nationkey', 'integer'),
                                    ('customer', 'c_phone', 'character'),
                                    ('customer', 'c_acctbal', 'numeric'),
                                    ('customer', 'c_mktsegment', 'character'),
                                    ('customer', 'c_comment', 'character varying'),
                                    ('orders', 'o_orderkey', 'integer'),
                                    ('orders', 'o_custkey', 'integer'),
                                    ('orders', 'o_orderstatus', 'character'),
                                    ('orders', 'o_totalprice', 'numeric'),
                                    ('orders', 'o_orderdate', 'date'),
                                    ('orders', 'o_orderpriority', 'character'),
                                    ('orders', 'o_clerk', 'character'),
                                    ('orders', 'o_shippriority', 'integer'),
                                    ('orders', 'o_comment', 'character varying')]

        self.do_init()
        q_gen.get_datatype = self.get_datatype

        q_gen._workingCopy.makeCopy(testDetails)
        eq = q_gen.formulate_nested_query_string('AVG(o_totalprice)',
                                                 ('orders', 'o_totalprice', "<=", -2147483648.88, 120000), 120000)
        print(eq)
        self.conn.closeConnection()
        self.assertTrue(eq is not None)


if __name__ == '__main__':
    unittest.main()
