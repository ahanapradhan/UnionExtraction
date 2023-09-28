from psycopg2 import Error

from .TpchSanitizer import TpchSanitizer
from ....refactored.abstract.ExtractorBase import Base
from ....refactored.executable import Executable
from ....refactored.util.common_queries import drop_table, get_row_count, drop_view


class Comparator(Base, TpchSanitizer):
    def __init__(self, connectionHelper, name, isHash):
        super().__init__(connectionHelper, name)
        self.isHash = isHash
        self.smaller_match_threshold = 5000
        self.app = Executable(connectionHelper)

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doActualJob(self, args):
        Q_h, Q_E = self.extract_params_from_args(args)
        self.sanitize()
        matched = self.check_matching(Q_h, Q_E)
        return matched

    def check_matching(self, Q_h, Q_E):
        if self.isHash:
            matched = self.match_hash_based(Q_h, Q_E)
        else:
            matched = self.match_comparison_based(Q_h, Q_E)
        return matched

    def create_view_from_Q_E(self, Q_E):
        try:
            self.logger.debug(Q_E)
            # Run the extracted query Q_E .
            self.connectionHelper.execute_sql([drop_view("temp1"), 'create view temp1 as ' + Q_E])
        except Error as e:
            self.logger.error("Extracted Query is not valid!", e)
            return False

        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0(get_row_count("temp1"))
        return res

    def run_except_query_match_and_dropViews(self):
        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0(get_row_count("temp2"))
        self.logger.debug("temp2", res)

        len1 = self.connectionHelper.execute_sql_fetchone_0(
            'select count(*) from (select * from temp1 except all select * from temp2) as T;')
        len2 = self.connectionHelper.execute_sql_fetchone_0(
            'select count(*) from (select * from temp2 except all select * from temp1) as T;')

        self.connectionHelper.execute_sql([drop_view("temp1"), drop_table("temp2")])

        if not len1 and not len2:
            return True
        else:
            return False

    def create_table_from_Qh(self, Q_h):
        # Create an empty table with name temp2
        self.connectionHelper.execute_sql([drop_table("temp2"), 'Create unlogged table temp2 (like temp1);'])
        result = self.app.doJob(Q_h)
        self.insert_data_into_Qh_table(result)

    def match_hash_based(self, Q_h, Q_E):
        return True

    def match_comparison_based(self, Q_h, Q_E):
        count_star_Q_E = self.create_view_from_Q_E(Q_E)
        self.logger.debug(count_star_Q_E)

        if not count_star_Q_E:
            return False

        self.create_table_from_Qh(Q_h)
        """
        if count_star_Q_E < self.smaller_match_threshold:
            check = self.check_smaller_match(Q_E, res_Qh)
            if not check:
                return False
        """
        check = self.run_except_query_match_and_dropViews()
        return check

    def insert_into_temp2_values(self, header, values):
        header_ = str(header).replace('\'', '')
        str_values = str(values)
        if str_values.startswith('('):
            self.connectionHelper.execute_sql(['INSERT INTO temp2' + header_ + ' VALUES ' + str(values) + '; '])
        else:
            self.connectionHelper.execute_sql(['INSERT INTO temp2' + header_ + ' VALUES (' + str(values) + '); '])

    def insert_data_into_Qh_table(self, res_Qh):
        pass

    def check_smaller_match(self, res_Q_E, res_Qh):
        pass
