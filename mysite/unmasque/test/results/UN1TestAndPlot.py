import os
import unittest

import pytest
from pygnuplot import gnuplot

from mysite.unmasque.src.core.executables.executable import Executable
from mysite.unmasque.src.core.result_comparator import ResultComparator
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.src.validator import validate_gb, validate_ob, Ob_suffix, count_ob, same_hidden_ob, \
    pretty_print
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from mysite.unmasque.test.results.tpch_kapil_report import Q1, Q2, Q4, Q5, Q6, Q11, Q10, Q3, Q16, Q17, Q18, Q21, Q16_nep, Q3_1, Q16_nep_2, \
    Q_r, Q_dt


class TpchExtractionPipelineTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.app = Executable(self.conn)
        self.extracted_U = "un1_queries"
        self.dat_filename = "un1_queries.dat"
        self.plot_script = "un1_queries.gnu"
        self.plot_filename = "un1_queries_plot.eps"
        self.latex_filename = "un1_queries_table.tex"
        self.summary_filename = "un1_extraction_summary.txt"
        self.gb_correct = False
        self.ob_correct = False
        self.result_correct = False
        self.ob_suffix = False
        self.same_hidden_ob = True
        self.count_ob = False
        self.ob_remark = "-"
        self.QNO_GBCORRECT_HEADER = "Qno                Gb Correct?"
        self.space_len = len(self.QNO_GBCORRECT_HEADER)

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

            self.add_extraction_summary(self.hq_keys[0])

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
        self.create_pipeline()
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

        self.gb_correct = validate_gb(query, u_Q)
        ob_dict = validate_ob(query, u_Q)
        self.same_hidden_ob = ob_dict[same_hidden_ob]
        self.ob_suffix = ob_dict[Ob_suffix]
        self.count_ob = ob_dict[count_ob]
        self.ob_correct = self.same_hidden_ob and (not self.ob_suffix) and (not self.count_ob)

        return t_aggregate, t_groupby, t_limit, t_orderby, t_projection, t_sampling, t_union, t_from_clause, t_view_min, t_where_clause

    def create_pipeline(self):
        self.pipeline = ExtractionPipeLine(self.conn)

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

    # @pytest.mark.skip
    def test_plot_Q1(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q1]
        self.hq_keys = ["Q1"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q2(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q2]
        self.hq_keys = ["Q2"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q3(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q3]
        self.hq_keys = ["Q3"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q3_1(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q3_1]
        self.hq_keys = ["Q3_1"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q4(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q4]
        self.hq_keys = ["Q4"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q5(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q5]
        self.hq_keys = ["Q5"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q6(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q6]
        self.hq_keys = ["Q6"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q10(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q10]
        self.hq_keys = ["Q10"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q11(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q11]
        self.hq_keys = ["Q11"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q16(self):
        self.conn.config.detect_nep = True
        self.hqs = [Q16]
        self.hq_keys = ["Q16"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q16_nep(self):
        self.conn.config.detect_nep = True
        self.hqs = [Q16_nep]
        self.hq_keys = ["Q16_nep"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q16_nep_2(self):
        self.conn.config.detect_nep = True
        self.hqs = [Q16_nep_2]
        self.hq_keys = ["Q16_nep_2"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q17(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q17]
        self.hq_keys = ["Q17"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q18(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q18]
        self.hq_keys = ["Q18"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q21(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q21]
        self.hq_keys = ["Q21"]
        self.do_experiment()

    # @pytest.mark.skip
    def test_plot_Q_r(self):
        self.conn.config.detect_nep = True
        self.hqs = [Q_r]
        self.hq_keys = ["Q_r"]
        self.do_experiment()

    def test_plot_Q_dt(self):
        self.conn.config.detect_nep = False
        self.hqs = [Q_dt]
        self.hq_keys = ["Q_dt"]
        self.do_experiment()

    @pytest.fixture(scope="session", autouse=True)
    def do_something(self):
        if os.path.isfile(self.summary_filename):
            os.remove(self.summary_filename)

        with open(self.summary_filename, "a") as myfile:
            myfile.write(f"{self.QNO_GBCORRECT_HEADER}     "
                         f"Ob Correct?     "
                         f"Same Ob_h?     "
                         f"count(*) ob?     "
                         f"Ob_suffix?     "
                         f"Result Correct?\n")

    def get_hspace(self, header, value):
        if value:
            correct_flag = f"{value} "
        else:
            correct_flag = f"{value}"
        hspace_len = len(header) - len(correct_flag)
        hspace = ""
        for i in range(hspace_len):
            hspace += " "
        return f"{correct_flag}{hspace}"

    def add_extraction_summary(self, hq_key):
        with open(self.summary_filename, "a") as myfile:
            prefix = pretty_print(hq_key, self.gb_correct)
            hspace_len = len("Gb_correct?     ") - 5
            hspace1 = ""
            for i in range(hspace_len):
                hspace1 += " "
            entry2 = self.get_hspace("Ob_correct?     ", self.ob_correct)
            entry3 = self.get_hspace("Same Ob_h?     ", self.same_hidden_ob)
            entry4 = self.get_hspace("count(*) ob?     ", self.count_ob)
            entry5 = self.get_hspace("Ob_suffix?     ", self.ob_suffix)
            entry6 = self.get_hspace("Result Correct?", self.result_correct)
            myfile.write(f"{prefix}{hspace1}"
                         f"{entry2}"
                         f"{entry3}"
                         f"{entry4}"
                         f"{entry5}"
                         f"{entry6}\n")

    # @pytest.mark.skip
    def test_revise_extraction_summary(self):
        self.hqs = [Q1, Q2, Q4, Q5, Q6, Q11, Q10, Q3, Q16, Q17, Q18, Q21, Q16_nep, Q3_1, Q16_nep_2, Q_r, Q_dt]
        self.hq_keys = ["Q1", "Q2", "Q4", "Q5", "Q6", "Q11", "Q10", "Q3", "Q16", "Q17", "Q18", "Q21",
                        "Q16_nep", "Q3_1", "Q16_nep_2", "Q_r", "Q_dt"]
        for idx in range(len(self.hqs)):
            query = self.hqs[idx]
            q_e_file = "/e_" + self.hq_keys[idx] + ".sql"
            with open(self.extracted_U + q_e_file) as e_q_filename:
                q_e = e_q_filename.read()

                rc = ResultComparator(self.conn, True)
                self.conn.connectUsingParams()
                self.result_correct = rc.doJob(query, q_e)
                if self.result_correct is None:
                    self.result_correct = False
                self.conn.closeConnection()

                self.gb_correct = validate_gb(query, q_e)
                ob_dict = validate_ob(query, q_e)
                self.same_hidden_ob = ob_dict[same_hidden_ob]
                self.ob_suffix = ob_dict[Ob_suffix]
                self.count_ob = ob_dict[count_ob]
                self.ob_correct = self.same_hidden_ob and (not self.ob_suffix) and (not self.count_ob)

                self.add_extraction_summary(self.hq_keys[idx])


if __name__ == '__main__':
    unittest.main()
