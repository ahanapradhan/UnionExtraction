from psycopg2 import Error

from ....refactored.abstract.ExtractorBase import Base
from ....refactored.executable import Executable
from ....refactored.util.common_queries import drop_table, get_row_count, drop_view, \
    get_star_from_except_all_get_star_from, create_view_as


class Comparator(Base):
    r_e = "r_e"
    r_h = "r_h"

    def __init__(self, connectionHelper, name, earlyExit):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)
        self.earlyExit = earlyExit

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doActualJob(self, args):
        Q_h, Q_E = self.extract_params_from_args(args)
        self.sanitize()
        if Q_E is None:
            self.logger.info("Got None to compared. Cannot do anything...sorry!")
            return False
        matched = self.match(Q_h, Q_E)
        return matched

    def create_view_from_Q_E(self, Q_E):
        try:
            self.logger.debug(Q_E)
            # Run the extracted query Q_E .
            self.connectionHelper.execute_sql([drop_view(self.r_e), create_view_as(self.r_e, Q_E)])
        except Error as e:
            self.logger.error(e)
            return False

        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0(get_row_count(self.r_e))
        return res

    def run_diff_query_match_and_dropViews(self):
        len1, len2 = self.run_diff_queries()
        self.logger.debug(len1, len2)
        self.connectionHelper.execute_sql([drop_view(self.r_e), drop_table(self.r_h)])
        return self.is_match(len1, len2)

    def is_match(self, len1, len2):
        if not len1 and not len2:
            return True
        else:
            return False

    def run_diff_queries(self):
        len1 = self.connectionHelper.execute_sql_fetchone_0(
            "select count(*) from " + get_star_from_except_all_get_star_from(self.r_e, self.r_h) + " as T;")
        len2 = self.connectionHelper.execute_sql_fetchone_0(
            "select count(*) from " + get_star_from_except_all_get_star_from(self.r_h, self.r_e) + " as T;")
        return len1, len2

    def create_table_from_Qh(self, Q_h):
        # Create an empty table with name temp2
        self.connectionHelper.execute_sql([drop_table(self.r_h),
                                           f"Create unlogged table {self.r_h} (like {self.r_e});"])
        result = self.app.doJob(Q_h)
        self.insert_data_into_Qh_table(result)

    def match(self, Q_h, Q_E):
        if Q_E is None:
            self.logger.debug("Q_E is none. Please see why.")
        count_star_Q_E = self.create_view_from_Q_E(Q_E)
        self.logger.debug(count_star_Q_E)

        if count_star_Q_E is None:
            return None

        if self.earlyExit and not count_star_Q_E:
            return False

        self.create_table_from_Qh(Q_h)

        # Size of the table
        count_star_Q_h = self.connectionHelper.execute_sql_fetchone_0(get_row_count(self.r_h))
        self.logger.debug(self.r_h, count_star_Q_h)

        if count_star_Q_E != count_star_Q_h:
            return False

        check = self.run_diff_query_match_and_dropViews()
        return check

    def insert_into_r_h_values(self, header, values):
        header_ = str(header).replace('\'', '')
        header_ = header_.replace(',)', ')')
        str_values = str(values)
        str_values = str_values.replace(",)", ")")
        if not str_values.startswith('('):
            str_values = f"({str_values})"
        self.connectionHelper.execute_sql([f"INSERT INTO {self.r_h}{header_} VALUES {str_values};"])

    def insert_data_into_Qh_table(self, res_Qh):
        # Filling the table temp2
        header = res_Qh[0]
        for i in range(1, len(res_Qh)):
            self.insert_into_r_h_values(header, res_Qh[i])
