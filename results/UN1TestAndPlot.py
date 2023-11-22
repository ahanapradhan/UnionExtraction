import os
import unittest

import pytest
from pygnuplot import gnuplot

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.src.validator import validate_gb_ob_attributes
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from results.tpch_kapil_report import Q1, Q2, Q4, Q5, Q6, Q11, Q10, Q3, Q16, Q17, Q18, Q21


def validate_gb_ob(q_h, q_e):
    valid = validate_gb_ob_attributes(q_h, q_e)
    return valid


class MyTestCase(BaseTestCase):
    extracted_U = "un1_queries"
    dat_filename = "un1_queries.dat"
    plot_script = "un1_queries.gnu"
    plot_filename = "un1_queries_plot.eps"
    latex_filename = "un1_queries_table.tex"
    summary_filename = "un1_extraction_summary.txt"

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.app = Executable(self.conn)
        self.gb_ob_correct = False
        self.result_correct = False

    def create_latex_table_of_queries(self):
        if os.path.isfile(self.latex_filename):
            os.remove(self.latex_filename)

        print(self.hqs)

        eqs = []
        # Iterate directory
        for path in os.listdir(self.extracted_U):
            # check if current path is a file
            if os.path.isfile(os.path.join(self.extracted_U, path)):
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
                hidden_query = hq
                hidden_query = hidden_query.replace("_", "\_")
                hidden_query = hidden_query.replace("%", "\%")
                print(hidden_query)
                expt.write(hidden_query + "&\n")

                with open(os.path.join(self.extracted_U, "e_" + self.hq_keys[i] + ".sql"), 'r') as file:
                    content = file.read()
                    splited_data = content.splitlines()
                    extracted_query = ' '.join(splited_data)
                    extracted_query = extracted_query.replace("_", "\_")
                    extracted_query = extracted_query.replace("%", "\%")
                print(extracted_query)
                expt.write(extracted_query + "\\\\\hline\n")
                i += 1

            expt.write("\\end{longtable}}")

    def do_experiment(self, ITERATIONS=1):
        self.do_dat_file_init()

        if os.path.isfile(self.summary_filename):
            os.remove(self.summary_filename)

        """
        read each query file and store the content in a string. 
        This string is the hidden query for the experiment
        """
        idx = 0
        for hq in self.hqs:
            q_time, query = self.record_hidden_query_exe_time(hq)

            t_sampling = 0
            t_view_min = 0
            t_where_clause = 0
            t_projection = 0
            t_groupby = 0
            t_aggregate = 0
            t_orderby = 0
            t_limit = 0
            t_result_com = 0
            t_union = 0
            t_from_clause = 0

            for i in range(ITERATIONS):
                t_aggregate, t_groupby, t_limit, t_orderby, t_projection, t_sampling, t_union, t_from_clause, t_view_min, t_where_clause = self.extract_query_once(
                    i, query, str(self.hq_keys[idx] + ".sql"), t_aggregate, t_groupby, t_limit, t_orderby, t_projection,
                    t_sampling, t_union, t_from_clause,
                    t_view_min, t_where_clause)

            dat_line = self.prepare_data(ITERATIONS, q_time, str(self.hq_keys[idx] + ".sql"), t_aggregate, t_groupby,
                                         t_limit, t_orderby,
                                         t_projection, t_sampling, t_union, t_from_clause, t_view_min, t_where_clause)

            with open(self.dat_filename, "a") as myfile:
                myfile.write(dat_line)

            self.add_extraction_summary(hq, self.gb_ob_correct, self.result_correct)

            idx += 1


        self.create_gnuplot()

        # self.assertTrue(os.path.isfile(self.plot_filename))  # add assertion here

    def do_dat_file_init(self):
        if not os.path.exists(self.extracted_U):
            os.makedirs(self.extracted_U)
        if os.path.isfile(self.dat_filename):
            os.remove(self.dat_filename)
        with open(self.dat_filename, "a") as myfile:
            myfile.write("#    q    union    from    cs2    vm    wc    pj    gb    agg    ob    lim\n")

    def record_hidden_query_exe_time(self, clean_content):
        query = clean_content
        self.conn.connectUsingParams()
        q_output = self.app.doJob(query)
        q_time = self.app.local_elapsed_time
        self.conn.closeConnection()
        return q_time, query

    def prepare_data(self, ITERATIONS,
                     q_time, sql,
                     t_aggregate,
                     t_groupby,
                     t_limit,
                     t_orderby,
                     t_projection,
                     t_sampling,
                     t_union,
                     t_from_clause,
                     t_view_min,
                     t_where_clause):
        sql = sql.replace(".sql", "")
        dat_line = sql
        dat_line += "    " + str(float("{:.2f}".format(q_time)))
        dat_line += "    " + str(float("{:.2f}".format(t_union / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_from_clause / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_sampling / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_view_min / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_where_clause / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_projection / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_groupby / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_aggregate / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_orderby / ITERATIONS)))
        dat_line += "    " + str(float("{:.2f}".format(t_limit / ITERATIONS)))
        # dat_line += "    " + str(float("{:.2f}".format(t_result_com/ITERATIONS)))
        dat_line += "\n"
        return dat_line

    def extract_query_once(self, i, query, sql, t_aggregate, t_groupby, t_limit, t_orderby, t_projection, t_sampling,
                           t_union, t_from_clause, t_view_min, t_where_clause):
        self.pipeline = ExtractionPipeLine(self.conn)
        u_Q = self.pipeline.doJob(query)
        print(u_Q)
        if not i:
            with open(self.extracted_U + "/e_" + sql, "w") as myfile:
                myfile.write(u_Q)
        t_from_clause += self.pipeline.time_profile.t_from_clause
        t_sampling += self.pipeline.time_profile.t_sampling
        t_view_min += self.pipeline.time_profile.t_view_min
        t_where_clause += self.pipeline.time_profile.t_where_clause
        t_projection += self.pipeline.time_profile.t_projection
        t_groupby += self.pipeline.time_profile.t_groupby
        t_aggregate += self.pipeline.time_profile.t_aggregate
        t_orderby += self.pipeline.time_profile.t_orderby
        t_limit += self.pipeline.time_profile.t_limit
        t_union += self.pipeline.time_profile.t_union

        self.result_correct = self.pipeline.correct

        check = validate_gb_ob(query, u_Q)
        self.gb_ob_correct = check

        return t_aggregate, t_groupby, t_limit, t_orderby, t_projection, t_sampling, t_union, t_from_clause, t_view_min, t_where_clause

    def create_gnuplot(self):

        self.delete_old_plot_files()

        with open(self.plot_script, "a") as myfile:
            myfile.write("set term eps\n")
            myfile.write("set output \"" + self.plot_filename + "\"\n")
            myfile.write("set style data histograms\n")
            myfile.write("set style histogram rowstacked\n")
            myfile.write("set boxwidth 0.4 relative\n")
            myfile.write("set style fill solid 1.0 border -1\n")
            myfile.write("set ylabel 'Extraction Time (ms)'\n")
            myfile.write("plot \'" + self.dat_filename + "\' using 2 t \"exe \","
                         + "\'\' using 3:xticlabels(1) t \"Union\", \'\' using 4:xticlabels(1) t \"From\", "
                           "\'\' using 5:xticlabels(1) t \"cs2\", "
                           "\'\' using 6:xticlabels(1) t \"View min\", \'\' using 7:xticlabels(1) t \"where\","
                           " \'\' using 8:xticlabels(1) t \"projection\", \'\' using 9:xticlabels(1) t \"group by\",  "
                           "\'\' using 10:xticlabels(1) t \"agg\", \'\' using 11:xticlabels(1) t \"order by\", "
                           "\'\' using 12:xticlabels(1) t \"limit\" lc \"coral\"\n")

        g = gnuplot.Gnuplot()

        g.cmd('set term eps')
        g.cmd('set output \"' + self.plot_filename + '\"')
        g.cmd('set style data histograms')
        g.cmd('set style histogram rowstacked')
        g.cmd('set boxwidth 0.4 relative')
        g.cmd('set style fill solid 1.0 border -1')
        g.cmd('set ylabel "Extraction Time (ms)"')
        g.cmd('plot \"' + self.dat_filename
              + '\" using 2 t "exe",\'\' using 3:xticlabels(1) t "Union", \'\' using 3:xticlabels(1) t "From",'
                '\'\' using 5:xticlabels(1) t "cs2", '
                '\'\' using 6:xticlabels(1) t "View min", \'\' using 7:xticlabels(1) t "where",'
                ' \'\' using 8:xticlabels(1) t "projection", \'\' using 9:xticlabels(1) t "group by",  '
                '\'\' using 10:xticlabels(1) t "agg", \'\' using 11:xticlabels(1) t "order by",'
                '\'\' using 12:xticlabels(1) t "limit" lc "coral"')

    def delete_old_plot_files(self):
        if os.path.isfile(self.plot_script):
            os.remove(self.plot_script)
        if os.path.isfile(self.plot_filename):
            os.remove(self.plot_filename)

    def test_plot_Q1(self):
        self.hqs = [Q1]
        self.hq_keys = ["Q1"]
        self.do_experiment()

    def test_plot_Q2(self):
        self.hqs = [Q2]
        self.hq_keys = ["Q2"]
        self.do_experiment()

    def test_plot_Q3(self):
        self.hqs = [Q3]
        self.hq_keys = ["Q3"]
        self.do_experiment()

    def test_plot_Q4(self):
        self.hqs = [Q4]
        self.hq_keys = ["Q4"]
        self.do_experiment()

    def test_plot_Q5(self):
        self.hqs = [Q5]
        self.hq_keys = ["Q5"]
        self.do_experiment()

    def test_plot_Q6(self):
        self.hqs = [Q6]
        self.hq_keys = ["Q6"]
        self.do_experiment()

    def test_plot_Q10(self):
        self.hqs = [Q10]
        self.hq_keys = ["Q10"]
        self.do_experiment()

    def test_plot_Q11(self):
        self.hqs = [Q11]
        self.hq_keys = ["Q11"]
        self.do_experiment()

    def test_plot_Q16(self):
        self.hqs = [Q16]
        self.hq_keys = ["Q16"]
        self.do_experiment()

    def test_plot_Q17(self):
        self.hqs = [Q17]
        self.hq_keys = ["Q17"]
        self.do_experiment()

    def test_plot_Q18(self):
        self.hqs = [Q18]
        self.hq_keys = ["Q18"]
        self.do_experiment()

    def test_plot_Q21(self):
        self.hqs = [Q21]
        self.hq_keys = ["Q21"]
        self.do_experiment()

    @pytest.mark.skip
    def test_plot(self):
        self.hqs = [Q3]
        self.hq_keys = ["Q3"]
        # self.hqs = [Q1, Q2, Q3, Q4, Q5, Q6, Q10, Q11, Q16, Q17, Q18, Q21]
        # self.hq_keys = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q10", "Q11", "Q16", "Q17", "Q18", "Q21"]
        self.do_experiment()
        # self.create_gnuplot()
        # self.create_latex_table_of_queries()

    def add_extraction_summary(self, hq_key, gb_ob_correct, result_correct):
        with open(self.summary_filename, "a") as myfile:
            myfile.write(f"{hq_key}\t{gb_ob_correct}\t{result_correct}\n")


if __name__ == '__main__':
    unittest.main()
