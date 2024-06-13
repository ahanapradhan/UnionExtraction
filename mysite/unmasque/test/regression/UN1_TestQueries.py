import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.results.tpch_kapil_report import Q1, Q3, Q2, Q10, Q11, Q21, Q18, Q6, \
    Q5, Q4, Q17, Q16
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from mysite.unmasque.test.util.gnp_testqueries import GQ1, GQ2, GQ3, GQ4, GQ5, GQ6, GQ7, GQ8, \
    GQ9, GQ10, GQ11, GQ12, GQ13, GQ14, GQ15, GQ16, GQ17, GQ18, GQ19, GQ20, GQ21, GQ22, GQ23, GQ24, \
    GQ25, GQ26, GQ27, GQ28, GQ29, GQ30, GQ31, GQ32, GQ33, GQ34, GQ35, GQ36, GQ37, GQ38, GQ39, GQ40, \
    GQ41, GQ42, GQ43, GQ44, GQ45, GQ46, GQ47, GQ48, GQ49, GQ50, GQ51, GQ52, GQ53, GQ54, GQ55, GQ56


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

    def test_plot1_Q1(self):
        query = Q1
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot2_Q2(self):
        query = Q2
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot3_Q3(self):
        query = Q3
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot4_Q4(self):
        query = Q4
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot5_Q5(self):
        query = Q5
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot6_Q6(self):
        query = Q6
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot7_Q10(self):
        query = Q10
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot8_Q11(self):
        query = Q11
        self.do_test(query)

    def test_plot9_Q16(self):
        query = Q16
        self.do_test(query)

    def test_plot10_Q17(self):
        query = Q17
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot11_Q18(self):
        query = Q18
        self.do_test(query)

    # @pytest.mark.skip
    def test_plot12_Q21(self):
        query = Q21
        self.do_test(query)

    def test_plot13_GQ1(self):
        query = GQ1
        self.do_test(query)

    def test_plot14_GQ2(self):
        query = GQ2
        self.do_test(query)

    def test_plot15_GQ3(self):
        query = GQ3
        self.do_test(query)

    def test_plot16_GQ4(self):
        query = GQ4
        self.do_test(query)

    def test_plot17_GQ5(self):
        query = GQ5
        self.do_test(query)

    def test_plot18_GQ6(self):
        query = GQ6
        self.do_test(query)

    def test_plot19_GQ7(self):
        query = GQ7
        self.do_test(query)

    def test_plot20_GQ8(self):
        query = GQ8
        self.do_test(query)

    def test_plot21_GQ9(self):
        query = GQ9
        self.do_test(query)

    def test_plot22_GQ10(self):
        query = GQ10
        self.do_test(query)

    def test_plot23_GQ11(self):
        query = GQ11
        self.do_test(query)

    def test_plot24_GQ12(self):
        query = GQ12
        self.do_test(query)

    def test_plot25_GQ13(self):
        query = GQ13
        self.do_test(query)

    def test_plot26_GQ14(self):
        query = GQ14
        self.do_test(query)

    def test_plot27_GQ15(self):
        query = GQ15
        self.do_test(query)

    def test_plot28_GQ16(self):
        query = GQ16
        self.do_test(query)

    def test_plot29_GQ17(self):
        query = GQ17
        self.do_test(query)

    def test_plot30_GQ18(self):
        query = GQ18
        self.do_test(query)

    def test_plot31_GQ19(self):
        query = GQ19
        self.do_test(query)

    def test_plot32_GQ20(self):
        query = GQ20
        self.do_test(query)

    def test_plot33_GQ21(self):
        query = GQ21
        self.do_test(query)

    def test_plot34_GQ22(self):
        query = GQ22
        self.do_test(query)

    def test_plot35_GQ23(self):
        query = GQ23
        self.do_test(query)

    def test_plot36_GQ24(self):
        query = GQ24
        self.do_test(query)

    def test_plot37_GQ25(self):
        query = GQ25
        self.do_test(query)

    def test_plot38_GQ26(self):
        query = GQ26
        self.do_test(query)

    def test_plot39_GQ27(self):
        query = GQ27
        self.do_test(query)

    def test_plot40_GQ28(self):
        query = GQ28
        self.do_test(query)

    def test_plot41_GQ29(self):
        query = GQ29
        self.do_test(query)

    def test_plot42_GQ30(self):
        query = GQ30
        self.do_test(query)

    def test_plot43_GQ31(self):
        query = GQ31
        self.do_test(query)

    def test_plot44_GQ32(self):
        query = GQ32
        self.do_test(query)

    def test_plot45_GQ33(self):
        query = GQ33
        self.do_test(query)

    def test_plot46_GQ34(self):
        query = GQ34
        self.do_test(query)

    def test_plot47_GQ35(self):
        query = GQ35
        self.do_test(query)

    def test_plot48_GQ36(self):
        query = GQ36
        self.do_test(query)

    def test_plot49_GQ37(self):
        query = GQ37
        self.do_test(query)

    def test_plot50_GQ38(self):
        query = GQ38
        self.do_test(query)

    def test_plot51_GQ39(self):
        query = GQ39
        self.do_test(query)

    def test_plot52_GQ40(self):
        query = GQ40
        self.do_test(query)

    def test_plot53_GQ41(self):
        query = GQ41
        self.do_test(query)

    def test_plot54_GQ42(self):
        query = GQ42
        self.do_test(query)

    def test_plot55_GQ43(self):
        query = GQ43
        self.do_test(query)

    def test_plot56_GQ44(self):
        query = GQ44
        self.do_test(query)

    def test_plot57_GQ45(self):
        query = GQ45
        self.do_test(query)

    def test_plot58_GQ46(self):
        query = GQ46
        self.do_test(query)

    def test_plot59_GQ47(self):
        query = GQ47
        self.do_test(query)

    def test_plot60_GQ48(self):
        query = GQ48
        self.do_test(query)

    def test_plot61_GQ49(self):
        query = GQ49
        self.do_test(query)

    def test_plot62_GQ50(self):
        query = GQ50
        self.do_test(query)

    def test_plot63_GQ51(self):
        query = GQ51
        self.do_test(query)

    def test_plot67_GQ52(self):
        query = GQ52
        self.do_test(query)

    def test_plot68_GQ53(self):
        query = GQ53
        self.do_test(query)

    def test_plot69_GQ54(self):
        query = GQ54
        self.do_test(query)

    def test_plot70_GQ55(self):
        query = GQ55
        self.do_test(query)

    def test_plot71_GQ56(self):
        query = GQ56
        self.do_test(query)


if __name__ == '__main__':
    unittest.main()
