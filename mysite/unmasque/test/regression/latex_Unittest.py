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


class MyTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.working_dir = "/home/kaptaan/Downloads/cpode/"
        self.extracted_ULatest = self.working_dir + "UN1_tests_new/"
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


    #@pytest.mark.skip
    def test_latex(self):
        self.create_latex_table_of_queries()

    def create_latex_table_of_queries(self):
        if os.path.isfile(self.latex_filename):
            os.remove(self.latex_filename)

        print(self.hqs)

        with open(self.latex_filename, 'a') as expt:
            expt.write("{\\small\n\\begin{longtable}{|p{0.6cm}|p{7cm}|p{7cm}|}\n"
                       "\\hline\n"
                       "{\\bf Q. No.} & {\\bf Old Result} & {\\bf New Result} \\\\\\hline\\hline\n")

            for i in range(len(self.hqs)):
                expt.write(self.hq_keys[i] + "&\n")
                try:
                    old_file = os.path.join(self.extracted_U_old, "e_" + self.hq_keys[i] + ".sql")
                    with open(old_file, 'r') as file:
                        content = file.read()
                        splited_data = content.splitlines()
                        extracted_query = ' '.join(splited_data)
                        extracted_query = extracted_query.replace("_", "\_")
                        extracted_query = extracted_query.replace("%", "\%")
                        print(extracted_query)
                        expt.write(extracted_query + "&\n")
                except FileNotFoundError as e:
                    expt.write("Fail" + "&\n")
                try:
                    new_file = os.path.join(self.extracted_ULatest, "e_" + self.hq_keys[i] + ".sql")
                    with open(new_file, 'r') as file:
                        content = file.read()
                        splited_data = content.splitlines()
                        extracted_query = ' '.join(splited_data)
                        extracted_query = extracted_query.replace("_", "\_")
                        extracted_query = extracted_query.replace("%", "\%")
                        print(extracted_query)
                        expt.write(extracted_query + "\\\\\hline\n")
                except FileNotFoundError as e:
                    expt.write("Fail" + "\\\\\hline\n")

            expt.write("\\end{longtable}}")



if __name__ == '__main__':
    unittest.main()
