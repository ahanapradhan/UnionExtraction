import time

from ....src.core.abstract.AppExtractorBase import AppExtractorBase
from ....src.core.db_restorer import DbRestorer


class Comparator(AppExtractorBase):
    r_e = "r_e"
    r_h = "r_h"

    def __init__(self, connectionHelper, name, earlyExit, core_relations=None):
        super().__init__(connectionHelper, name)
        self.relations = core_relations
        self.earlyExit = earlyExit
        self.row_count_r_e = 0
        self.row_count_r_h = 0
        self.db_restorer = DbRestorer(self.connectionHelper, self.relations)

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doActualJob(self, args=None):
        start_t = time.time()
        for tab in self.relations:
            tab_size = self.db_restorer.restore_table_and_confirm(tab)
            if not tab_size:
                self.logger.error(f"Could not restore {tab}, cannot run result comparator!")
                return None
        end_t = time.time()
        restore_time = end_t - start_t

        Q_h, Q_E = self.extract_params_from_args(args)
        if Q_E is None:
            self.logger.info("Got None to compare. Cannot do anything...sorry!")
            return None
        try:
            self.app.doJob(Q_E)
        except:
            self.logger.error("Q_E is not semantically correct.")
            return None

        matched = self.match(Q_h, Q_E)
        return matched, restore_time

    def create_view_from_Q_E(self, Q_E):
        try:
            self.logger.debug(Q_E)
            # Run the extracted query Q_E .
            self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(self.r_e),
                                               self.connectionHelper.queries.create_view_as(self.r_e, Q_E)])
        except ValueError as e:
            self.logger.error(e)
            return False

        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(self.r_e))
        return res

    def run_diff_query_match_and_dropViews(self):
        len1, len2 = self.run_diff_queries()
        self.logger.debug(len1, len2)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(self.r_e),
                                           self.connectionHelper.queries.drop_table(self.r_h)])
        return self.is_match(len1, len2)

    def is_match(self, len1, len2):
        if not len1 and not len2:
            return True
        else:
            return False

    def run_diff_queries(self):
        len1 = self.connectionHelper.execute_sql_fetchone_0(
            "select count(*) from " + self.connectionHelper.queries.get_star_from_except_all_get_star_from(self.r_e,
                                                                                                           self.r_h) + " as T;", self.logger)
        len2 = self.connectionHelper.execute_sql_fetchone_0(
            "select count(*) from " + self.connectionHelper.queries.get_star_from_except_all_get_star_from(self.r_h,
                                                                                                           self.r_e) + " as T;", self.logger)
        return len1, len2

    def create_table_from_Qh(self, Q_h):
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table(self.r_h),
                                           f"Create unlogged table {self.r_h} (like {self.r_e});"])
        result = self.app.doJob(Q_h)
        self.insert_data_into_Qh_table(result, self.r_h)

    def match(self, Q_h, Q_E):
        if Q_E is None:
            self.logger.error("Q_E is none. Please see why.")
            return False
        self.row_count_r_e = self.create_view_from_Q_E(Q_E)
        self.logger.debug(self.row_count_r_e)

        if self.row_count_r_e is None:
            return None

        if self.earlyExit and not self.row_count_r_e:
            return False

        self.create_table_from_Qh(Q_h)

        # Size of the table
        self.row_count_r_h = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.get_row_count(self.r_h))
        self.logger.debug(self.r_h, self.row_count_r_h)

        if self.row_count_r_e != self.row_count_r_h:
            return False

        check = self.run_diff_query_match_and_dropViews()
        return check

    def insert_into_result_table_values(self, header, values, table):
        header_ = str(header).replace('\'', '')
        header_ = header_.replace(',)', ')')
        str_values = str(values)
        str_values = str_values.replace(",)", ")")
        str_values = str_values.replace("\'NULL\'", "NULL")
        if not str_values.startswith('('):
            str_values = f"('{str_values}')"
        self.connectionHelper.execute_sql([f"INSERT INTO {table}{header_} VALUES {str_values};"])

    def insert_data_into_Qh_table(self, res_Qh, table):
        header = res_Qh[0]
        for i in range(1, len(res_Qh)):
            self.insert_into_result_table_values(header, res_Qh[i], table)
