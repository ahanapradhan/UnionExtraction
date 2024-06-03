import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.results.tpch_kapil_report import Q1, Q3, Q2, Q10, Q11, Q16, Q16_nep, Q16_nep_2, Q21, Q18, Q6, \
    Q5, Q4, Q17
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn.config.detect_union = False
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_or = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def do_test(self, query):
        u_Q = self.pipeline.doJob(query)
        print(u_Q)
        record_file = open("extraction_result.sql", "a")
        record_file.write("\n --- START OF ONE EXTRACTION EXPERIMENT\n")
        record_file.write(" --- input query:\n ")
        record_file.write(query)
        record_file.write("\n")
        record_file.write(" --- extracted query:\n ")
        if u_Q is None:
            u_Q = '--- Extraction Failed! Nothing to show! '
        record_file.write(u_Q)
        record_file.write("\n --- END OF ONE EXTRACTION EXPERIMENT\n")
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    def test_plot_Q1(self):
        query = Q1
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q2(self):
        query = Q2
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q3(self):
        query = Q3
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q4(self):
        query = Q4
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q5(self):
        query = Q5
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q6(self):
        query = Q6
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q10(self):
        query = Q10
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q11(self):
        query = Q11
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q16_nep(self):
        query = Q16_nep
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q16_nep_2(self):
        query = Q16_nep_2
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q17(self):
        query = Q17
        self.do_test(query)

    def test_Q3_gopi(self):
        query = "Select l_orderkey, sum(l_extendedprice) as revenue, o_orderdate, o_shippriority " \
                "From customer, orders, lineitem " \
                "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
                "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " \
                "Group By o_orderdate, l_orderkey, o_shippriority limit 10;"
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q18(self):
        query = Q18
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot_Q21(self):
        query = Q21
        self.do_test(query)


if __name__ == '__main__':
    unittest.main()
