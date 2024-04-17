import copy
import os

from mysite.unmasque.src.core.executable import Executable
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    extracted_U = "extracted_union_queries"
    dat_filename = "union_queries.dat"
    query_dir_path = "union_queries"
    plot_script = "union_queries.gnu"
    plot_filename = "union_queries_plot.eps"
    latex_filename = "queries_table.tex"
    testcase_filename = "paper_test.py"

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_union = True
        self.app = Executable(self.conn)

    def create_latex_table_of_queries(self):
        if os.path.isfile(self.latex_filename):
            os.remove(self.latex_filename)

        hqs = []
        # Iterate directory
        for path in os.listdir(self.query_dir_path):
            # check if current path is a file
            if os.path.isfile(os.path.join(self.query_dir_path, path)):
                hqs.append(path)
        print(hqs)

        eqs = []
        # Iterate directory
        for path in os.listdir(self.extracted_U):
            # check if current path is a file
            if os.path.isfile(os.path.join(self.extracted_U, path)):
                eqs.append(path)
        print(eqs)

        hqs.sort()
        eqs.sort()

        self.assertEqual(len(hqs), len(eqs))

        with open(self.latex_filename, 'a') as expt:
            expt.write("\\onecolumn\n"
                       "\\begin{center}\n"
                       "\\tablefirsthead{\\hline\\footnotesize {Q. No.} &	"
                       "\\footnotesize{Hidden Query \\Q}&\\footnotesize {Extracted Query $Q_E$} \\\\"
                       "\\hline}\n"
                       "\\tablehead{\\hline}\n"
                       "\\tabletail{\\hline}\n"
                       "\\tablelasttail{\\hline}\n"
                       "\\tablecaption{Evaluated Queries}\n"
                       "\\begin{supertabular}{|c|p{7cm}|p{7cm}|}\\hline")

            i = 0
            for hq in hqs:
                q_name = copy.deepcopy(hq)
                q_name = q_name.replace(".sql", "")
                expt.write(f"\\footnotesize{{{q_name}}} &\n")
                with open(os.path.join(self.query_dir_path, hq), 'r') as file:
                    content = file.read()
                    splited_data = content.splitlines()
                    hidden_query = ' '.join(splited_data)
                    hidden_query = hidden_query.replace("_", "\_")
                    hidden_query = hidden_query.replace("%", "\%")
                print(hidden_query)
                expt.write(f"\\footnotesize{{{hidden_query}}} &\n")

                with open(os.path.join(self.extracted_U, "e_" + hq), 'r') as file:
                    content = file.read()
                    splited_data = content.splitlines()
                    extracted_query = ' '.join(splited_data)
                    extracted_query = extracted_query.replace("_", "\_")
                    extracted_query = extracted_query.replace("%", "\%")
                print(extracted_query)
                expt.write(f"\\footnotesize{{{extracted_query}}} \\\\\\hline")

            expt.write("\\end{supertabular}\n\\label{result:union_aoa}\\end{center}\n\\twocolumn")

    def get_all_query_files(self):
        # list to store files
        res = []
        # Iterate directory
        for path in os.listdir(self.query_dir_path):
            # check if current path is a file
            if os.path.isfile(os.path.join(self.query_dir_path, path)):
                res.append(path)
        print(res)
        return res

    def get_query_content_from_file(self, sql):
        with open(os.path.join(self.query_dir_path, sql), 'r') as file:
            content = file.read()
            splited_data = content.splitlines()
            clean_content = ' '.join(splited_data)
        print(clean_content)
        return clean_content

    def do_generation(self, ITERATIONS=1):
        with open(self.testcase_filename, 'w') as file:
            file.write(f"import os\n"
                       f"from mysite.unmasque.refactored.executable import Executable\n"
                       f"from mysite.unmasque.src.pipeline.UnionPipeLine import UnionPipeLine\n"
                       f"from mysite.unmasque.test.util.BaseTestCase import BaseTestCase\n\n\n"
                       f"class MyTestCase(BaseTestCase):\n"
                       f"    def __init__(self, *args, **kwargs):\n"
                       f"        super(BaseTestCase, self).__init__(*args, **kwargs)\n"
                       f"        self.conn.config.detect_union = True\n"
                       f"        self.app = Executable(self.conn)\n\n")

            res = self.get_all_query_files()

            for sql in res:
                clean_content = self.get_query_content_from_file(sql)
                e_sql = copy.deepcopy(sql)
                sql = sql.replace(".", "_")

                testcase_text = \
                    f"    def test_{sql}(self):\n" \
                    f"        test_key = \"e_{e_sql}\"\n" \
                    f"        self.conn.connectUsingParams()\n" \
                    f"        query = \"{clean_content}\"\n" \
                    f"        self.pipeline = UnionPipeLine(self.conn)\n" \
                    f"        eq = self.pipeline.doJob(query)\n" \
                    f"        print(eq)\n" \
                    f"        self.assertTrue(eq is not None)\n" \
                    f"        self.conn.closeConnection()\n" \
                    f"        with open(os.path.join(\"{self.extracted_U}\", test_key), 'w') as file:\n" \
                    f"            file.write(eq)\n" \
                    f"        self.assertTrue(self.pipeline.correct)\n\n"

                file.write(testcase_text)

    def test_create_testcases(self):
        self.do_generation()

    def test_do_latex(self):
        self.create_latex_table_of_queries()
