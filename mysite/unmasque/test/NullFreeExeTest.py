import unittest

from mysite.unmasque.src.core.nullfree_executable import NullFreeExecutable
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory


def get_union_query_after_nullify_table(tab_list, first_join_type, second_join_type):
    nation = 'nation1' if 'nation' in tab_list else 'nation'
    region = 'region1' if 'region' in tab_list else 'region'
    customer = 'customer1' if 'customer' in tab_list else 'customer'
    query = f"select c_name, n_comment FROM {customer} {first_join_type} OUTER JOIN {nation} on c_nationkey = " \
            f"n_nationkey and " \
            f"c_acctbal < 2000 " \
            f"UNION ALL" \
            f" select n_name, r_comment FROM {nation} {second_join_type} OUTER JOIN {region} on n_regionkey = " \
            f"r_regionkey and r_name " \
            "= 'AFRICA';"
    print(query)
    return query


class MyTestCase(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.global_attrib_types_dict = {}
        self.global_attrib_types = None
        self.conn = ConnectionHelperFactory().createConnectionHelper()
        self.exe = NullFreeExecutable(self.conn)

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

    def test_resultset_outerJoin(self):
        self.conn.connectUsingParams()
        query = "select n_name, r_comment from nation inner join region on n_regionkey = r_regionkey and r_name = " \
                "'AFRICA';"
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_nonEmpty_nullfree(res))
        self.conn.execute_sql([self.conn.queries.create_table_like("region1", "region")])
        query = "select n_name, r_comment from nation inner join region1 on n_regionkey = r_regionkey and r_name = " \
                "'AFRICA';"
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_empty(res))
        query = "select n_name, r_comment from nation full outer join region on n_regionkey = r_regionkey and r_name " \
                "= 'AFRICA';"
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_nonEmpty_nullfree(res))
        self.assertFalse(self.exe.isQ_result_no_full_nullfree_row(res))
        query = "select n_name, r_comment from nation full outer join region1 on n_regionkey = r_regionkey and r_name " \
                "= 'AFRICA';"
        res = self.exe.doJob(query)
        self.assertFalse(self.exe.isQ_result_nonEmpty_nullfree(res))
        self.assertTrue(self.exe.isQ_result_no_full_nullfree_row(res))
        self.conn.execute_sql([self.conn.queries.drop_table("region1")])
        self.conn.closeConnection()

    def test_union_query_resultset(self):
        import itertools
        types = ['FULL', 'LEFT', 'RIGHT']
        combinations = list(itertools.product(types, repeat=2))
        self.conn.connectUsingParams()
        self.conn.execute_sql([self.conn.queries.create_table_like("nation1", 'nation'),
                               self.conn.queries.create_table_like("region1", 'region'),
                               self.conn.queries.create_table_like("customer1", 'customer')])
        for comb in combinations:
            print(comb)
            first, second = comb[0], comb[1]
            self.do_nullifications_of_tables(first, second)
        self.conn.execute_sql([self.conn.queries.drop_table("region1"),
                               self.conn.queries.drop_table("nation1"),
                               self.conn.queries.drop_table("customer1")])

        self.conn.closeConnection()

    def do_nullifications_of_tables(self, first, second):
        query = get_union_query_after_nullify_table([], first, second)
        res = self.exe.doJob(query)
        # self.assertFalse(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertFalse(self.exe.isQ_result_all_null(res))
        self.assertFalse(self.exe.isQ_result_has_no_data(res))
#        self.assertFalse(self.exe.isQ_result_empty(res))
        self.assertTrue(self.exe.isQ_result_has_some_data(res))
        # self.assertFalse(self.exe.isQ_result_nonEmpty_nullfree(res))
        query = get_union_query_after_nullify_table(['customer'], first, second)
        res = self.exe.doJob(query)
#        self.assertFalse(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertFalse(self.exe.isQ_result_all_null(res)
                         or self.exe.isQ_result_has_no_data(res)
                         or self.exe.isQ_result_empty(res))
        # self.assertTrue(self.exe.isQ_result_has_some_data(res))
        query = get_union_query_after_nullify_table(['nation'], first, second)
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertTrue(self.exe.isQ_result_all_null(res)
                        or self.exe.isQ_result_has_no_data(res)
                        or self.exe.isQ_result_empty(res))
        # self.assertTrue(self.exe.isQ_result_has_some_data(res))
        query = get_union_query_after_nullify_table(['region'], first, second)
        res = self.exe.doJob(query)
        self.assertFalse(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertFalse(self.exe.isQ_result_all_null(res)
                         or self.exe.isQ_result_has_no_data(res)
                         or self.exe.isQ_result_empty(res))
        # self.assertTrue(self.exe.isQ_result_has_some_data(res))
        query = get_union_query_after_nullify_table(['region', 'customer'], first, second)
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertTrue(self.exe.isQ_result_all_null(res)
                        or self.exe.isQ_result_has_no_data(res)
                        or self.exe.isQ_result_empty(res))
        query = get_union_query_after_nullify_table(['nation', 'customer'], first, second)
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertTrue(self.exe.isQ_result_all_null(res)
                        or self.exe.isQ_result_has_no_data(res)
                        or self.exe.isQ_result_empty(res))
        query = get_union_query_after_nullify_table(['region', 'nation'], first, second)
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertTrue(self.exe.isQ_result_all_null(res)
                        or self.exe.isQ_result_has_no_data(res)
                        or self.exe.isQ_result_empty(res))
        query = get_union_query_after_nullify_table(['region', 'customer', 'nation'], first, second)
        res = self.exe.doJob(query)
        self.assertTrue(self.exe.isQ_result_no_full_nullfree_row(res))
        self.assertTrue(self.exe.isQ_result_all_null(res)
                        or self.exe.isQ_result_has_no_data(res)
                        or self.exe.isQ_result_empty(res))
        self.assertFalse(self.exe.isQ_result_has_some_data(res))


if __name__ == '__main__':
    unittest.main()
