import os
import shutil
import unittest

import pytest

from ...src.core.factory.PipeLineFactory import PipeLineFactory
from ..results.tpch_kapil_report import Q1, Q3, Q2, Q10, Q11, Q21, Q18, Q6, Q5, Q4, Q17, Q16
from ..util.BaseTestCase import BaseTestCase
from ..util.gnp_testqueries import GQ1, GQ2, GQ3, GQ4, GQ5, GQ6, GQ7, GQ8, GQ9, GQ10, GQ11, GQ12, GQ13, GQ14, GQ15, \
    GQ16, GQ17, GQ18, GQ19, GQ20, GQ21, GQ22, GQ23, GQ24, GQ25, GQ26, GQ27, GQ28, GQ29, GQ30, GQ31, GQ32, GQ33, GQ34, \
    GQ35, GQ36, GQ37, GQ38, GQ39, GQ40, GQ41, GQ42, GQ43, GQ44, GQ45, GQ46, GQ47, GQ48, GQ49, GQ50, GQ51, GQ52, GQ53, \
    GQ54, GQ55, GQ56, GQ57, GQ58


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.working_dir = "unmasque/test/"
        self.extracted_ULatest = self.working_dir + "UN1_tests_new"
        self.hq_keys = ["Q1", "Q3", "Q2", "Q10", "Q11", "Q21", "Q18", "Q6", "Q5", "Q4", "Q17", "Q16",
                        "GQ1", "GQ2", "GQ3", "GQ4", "GQ5", "GQ6", "GQ7", "GQ8", "GQ9", "GQ10", "GQ11",
                        "GQ12", "GQ13", "GQ14", "GQ15", "GQ16", "GQ17", "GQ18", "GQ19", "GQ20", "GQ21",
                        "GQ22", "GQ23", "GQ24", "GQ25", "GQ26", "GQ27", "GQ28", "GQ29", "GQ30", "GQ31",
                        "GQ32", "GQ33", "GQ34", "GQ35", "GQ36", "GQ37", "GQ38", "GQ39", "GQ40", "GQ41",
                        "GQ42", "GQ43", "GQ44", "GQ45", "GQ46", "GQ47", "GQ48", "GQ49", "GQ50", "GQ51",
                        "GQ52", "GQ53", "GQ54", "GQ55", "GQ56", "GQ57", "GQ58"]
        self.hqs = [Q1, Q3, Q2, Q10, Q11, Q21, Q18, Q6, Q5, Q4, Q17, Q16,
                    GQ1, GQ2, GQ3, GQ4, GQ5, GQ6, GQ7, GQ8, GQ9, GQ10, GQ11, GQ12, GQ13, GQ14, GQ15, GQ16,
                    GQ17, GQ18, GQ19, GQ20, GQ21, GQ22, GQ23, GQ24, GQ25, GQ26, GQ27, GQ28, GQ29, GQ30, GQ31,
                    GQ32, GQ33, GQ34, GQ35, GQ36, GQ37, GQ38, GQ39, GQ40, GQ41, GQ42, GQ43, GQ44, GQ45, GQ46,
                    GQ47, GQ48, GQ49, GQ50, GQ51, GQ52, GQ53, GQ54, GQ55, GQ56, GQ57, GQ58]

        self.extracted_U_old = self.working_dir + "UN1_Tests_old"
        self.latex_filename = "ExpResults.tex"
        self.conn.config.detect_union = False
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_or = False
        self.pipeline = None
        self.do_setup()

    def do_setup(self):
        if os.path.exists(self.extracted_U_old):
            # If the folder exists, delete all the items in it
            for filename in os.listdir(self.extracted_U_old):
                file_path = os.path.join(self.extracted_U_old, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
        else:
            # If the folder doesn't exist, create it
            os.makedirs(self.extracted_U_old)
        print(f"The '{self.extracted_U_old}' folder has been created or cleared.")

    @pytest.mark.skip
    def test_latex(self):
        self.create_latex_table_of_queries()

    def do_test(self, query, key):
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        u_Q = self.pipeline.doJob(query)
        if not os.path.exists(self.extracted_U_old):
            os.makedirs(self.extracted_U_old)
        print(u_Q)
        record_file = open("extraction_result.sql", "a")
        record_file.write("\n --- START OF ONE EXTRACTION EXPERIMENT\n")
        record_file.write(" --- input query:\n ")
        record_file.write(query)
        record_file.write("\n")
        with open(self.extracted_U_old + "/e_" + key + ".sql", "w") as myfile:
            myfile.write(u_Q)
        record_file.write("\n --- END OF ONE EXTRACTION EXPERIMENT\n")
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    def create_latex_table_of_queries(self):
        if os.path.isfile(self.latex_filename):
            os.remove(self.latex_filename)

        print(self.hqs)

        eqs = []
        # Iterate directory
        for path in os.listdir(self.extracted_U_old):
            # check if current path is a file
            if os.path.isfile(os.path.join(self.extracted_U_old, path)):
                eqs.append(path)
        print(eqs)

        self.assertEqual(len(self.hqs), len(eqs))

        with open(self.latex_filename, 'a') as expt:
            expt.write("{\\tiny\n\\begin{longtable}{|p{0.5cm}|p{7cm}|p{7cm}|}\n"
                       "\\hline\n"
                       "{\\bf Q. No.} & {\\bf Hidden Query} & {\\bf Extracted Query} \\\\\\hline\\hline\n")

            i = 0
            for hq in self.hqs:
                expt.write(self.hq_keys[i] + "&\n")
                with open(os.path.join(self.extracted_U_old, "e_" + self.hq_keys[i] + ".sql"), 'r') as file:
                    content = file.read()
                    splited_data = content.splitlines()
                    extracted_query = ' '.join(splited_data)
                    extracted_query = extracted_query.replace("_", "\_")
                    extracted_query = extracted_query.replace("%", "\%")
                print(extracted_query)

                expt.write(extracted_query + "&\n")

                with open(os.path.join(self.extracted_ULatest, "e_" + self.hq_keys[i] + ".sql"), 'r') as file:
                    content = file.read()
                    splited_data = content.splitlines()
                    extracted_query = ' '.join(splited_data)
                    extracted_query = extracted_query.replace("_", "\_")
                    extracted_query = extracted_query.replace("%", "\%")
                print(extracted_query)
                expt.write(extracted_query + "\\\\\hline\n")
                i += 1

            expt.write("\\end{longtable}}")

    def test_plot1_Q1(self):
        query = Q1
        key = "Q1"
        self.do_test(query, key)

    # @pytest.mark.skip
    def test_plot2_Q2(self):
        query = Q2
        self.do_test(query, "Q2")

    # @pytest.mark.skip
    def test_plot3_Q3(self):
        query = Q3
        self.do_test(query, "Q3")

    # @pytest.mark.skip
    def test_plot4_Q4(self):
        query = Q4
        self.do_test(query, "Q4")

    # @pytest.mark.skip
    def test_plot5_Q5(self):
        query = Q5
        self.do_test(query, "Q5")

    # @pytest.mark.skip
    def test_plot6_Q6(self):
        query = Q6
        self.do_test(query, "Q6")

    # @pytest.mark.skip
    def test_plot7_Q10(self):
        query = Q10
        self.do_test(query, "Q10")

    # @pytest.mark.skip
    def test_plot8_Q11(self):
        query = Q11
        self.do_test(query, "Q11")

    def test_plot9_Q16(self):
        query = Q16
        self.do_test(query, "Q16")

    def test_plot10_Q17(self):
        query = Q17
        self.do_test(query, "Q17")

    # @pytest.mark.skip
    def test_plot11_Q18(self):
        query = Q18
        self.do_test(query, "Q18")

    # @pytest.mark.skip
    def test_plot12_Q21(self):
        query = Q21
        self.do_test(query, "Q21")

    def test_plot13_GQ1(self):
        query = GQ1
        self.do_test(query, "GQ1")

    def test_plot14_GQ2(self):
        query = GQ2
        self.do_test(query, "GQ2")

    def test_plot15_GQ3(self):
        query = GQ3
        self.do_test(query, "GQ3")

    def test_plot16_GQ4(self):
        query = GQ4
        self.do_test(query, "GQ4")

    def test_plot17_GQ5(self):
        query = GQ5
        self.do_test(query, "GQ5")

    def test_plot18_GQ6(self):
        query = GQ6
        self.do_test(query, "GQ6")

    def test_plot19_GQ7(self):
        query = GQ7
        self.do_test(query, "GQ7")

    def test_plot20_GQ8(self):
        query = GQ8
        self.do_test(query, "GQ8")

    def test_plot21_GQ9(self):
        query = GQ9
        self.do_test(query, "GQ9")

    def test_plot22_GQ10(self):
        query = GQ10
        self.do_test(query, "GQ10")

    def test_plot23_GQ11(self):
        query = GQ11
        self.do_test(query, "GQ11")

    def test_plot24_GQ12(self):
        query = GQ12
        self.do_test(query, "GQ12")

    def test_plot25_GQ13(self):
        query = GQ13
        self.do_test(query, "GQ13")

    def test_plot26_GQ14(self):
        query = GQ14
        self.do_test(query, "GQ14")

    def test_plot27_GQ15(self):
        query = GQ15
        self.do_test(query, "GQ15")

    def test_plot28_GQ16(self):
        query = GQ16
        self.do_test(query, "GQ16")

    def test_plot29_GQ17(self):
        query = GQ17
        self.do_test(query, "GQ17")

    def test_plot30_GQ18(self):
        query = GQ18
        self.do_test(query, "GQ18")

    def test_plot31_GQ19(self):
        query = GQ19
        self.do_test(query, "GQ19")

    def test_plot32_GQ20(self):
        query = GQ20
        self.do_test(query, "GQ20")

    def test_plot33_GQ21(self):
        query = GQ21
        self.do_test(query, "GQ21")

    def test_plot34_GQ22(self):
        query = GQ22
        self.do_test(query, "GQ22")

    def test_plot35_GQ23(self):
        query = GQ23
        self.do_test(query, "GQ23")

    def test_plot36_GQ24(self):
        query = GQ24
        self.do_test(query, "GQ24")

    def test_plot37_GQ25(self):
        query = GQ25
        self.do_test(query, "GQ25")

    def test_plot38_GQ26(self):
        query = GQ26
        self.do_test(query, "GQ26")

    def test_plot39_GQ27(self):
        query = GQ27
        self.do_test(query, "GQ27")

    def test_plot40_GQ28(self):
        query = GQ28
        self.do_test(query, "GQ28")

    def test_plot41_GQ29(self):
        query = GQ29
        self.do_test(query, "GQ29")

    def test_plot42_GQ30(self):
        query = GQ30
        self.do_test(query, "GQ30")

    def test_plot43_GQ31(self):
        query = GQ31
        self.do_test(query, "GQ31")

    def test_plot44_GQ32(self):
        query = GQ32
        self.do_test(query, "GQ32")

    def test_plot45_GQ33(self):
        query = GQ33
        self.do_test(query, "GQ33")

    def test_plot46_GQ34(self):
        query = GQ34
        self.do_test(query, "GQ34")

    def test_plot47_GQ35(self):
        query = GQ35
        self.do_test(query, "GQ35")

    def test_plot48_GQ36(self):
        query = GQ36
        self.do_test(query, "GQ36")

    def test_plot49_GQ37(self):
        query = GQ37
        self.do_test(query, "GQ37")

    def test_plot50_GQ38(self):
        query = GQ38
        self.do_test(query, "GQ38")

    def test_plot51_GQ39(self):
        query = GQ39
        self.do_test(query, "GQ39")

    def test_plot52_GQ40(self):
        query = GQ40
        self.do_test(query, "GQ40")

    def test_plot53_GQ41(self):
        query = GQ41
        self.do_test(query, "GQ41")

    def test_plot54_GQ42(self):
        query = GQ42
        self.do_test(query, "GQ42")

    def test_plot55_GQ43(self):
        query = GQ43
        self.do_test(query, "GQ43")

    def test_plot56_GQ44(self):
        query = GQ44
        self.do_test(query, "GQ44")

    def test_plot57_GQ45(self):
        query = GQ45
        self.do_test(query, "GQ45")

    def test_plot58_GQ46(self):
        query = GQ46
        self.do_test(query, "GQ46")

    def test_plot59_GQ47(self):
        query = GQ47
        self.do_test(query, "GQ47")

    def test_plot60_GQ48(self):
        query = GQ48
        self.do_test(query, "GQ48")

    def test_plot61_GQ49(self):
        query = GQ49
        self.do_test(query, "GQ49")

    def test_plot62_GQ50(self):
        query = GQ50
        self.do_test(query, "GQ50")

    def test_plot63_GQ51(self):
        query = GQ51
        self.do_test(query, "GQ51")

    def test_plot67_GQ52(self):
        query = GQ52
        self.do_test(query, "GQ52")

    def test_plot68_GQ53(self):
        query = GQ53
        self.do_test(query, "GQ53")

    def test_plot69_GQ54(self):
        query = GQ54
        self.do_test(query, "GQ54")

    def test_plot70_GQ55(self):
        query = GQ55
        self.do_test(query, "GQ55")

    def test_plot71_GQ56(self):
        query = GQ56
        self.do_test(query, "GQ56")

    def test_plot72_GQ56(self):
        query = GQ57
        self.do_test(query, "GQ57")

    def test_plot73_GQ58(self):
        query = GQ58
        self.do_test(query, "GQ58")


if __name__ == '__main__':
    unittest.main()