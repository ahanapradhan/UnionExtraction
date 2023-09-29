from psycopg2 import Error

from .util.common_queries import drop_view, get_row_count
from ..src.pipeline.abstract.Comparator import Comparator


def hashtext_query(tab):
    return "select sum(hashtext) from (select hashtext(" + tab + "::TEXT) FROM " + tab + ") as T;"


class ResultComparator(Comparator):
    hash_r_e = "r_e"
    hash_r_h = "r_h"

    def __init__(self, connectionHelper, isHash):
        super().__init__(connectionHelper, "Result Comparator")
        self.isHash = isHash

    def is_match(self, len1, len2):
        if self.isHash:
            if len1 == len2:
                return True
            else:
                return False
        else:
            return super().is_match(len1, len2)

    def match(self, Q_h, Q_E):
        if self.isHash:
            self.r_e = "r_e"
            self.r_h = "r_h"
        return super().match(Q_h, Q_E)

    def run_diff_queries(self):
        if self.isHash:
            return self.run_hash_diff_queries()
        return super().run_diff_queries()

    def run_hash_diff_queries(self):
        len1 = self.connectionHelper.execute_sql_fetchone_0(hashtext_query(self.r_e))
        len2 = self.connectionHelper.execute_sql_fetchone_0(hashtext_query(self.r_h))
        self.logger.debug("---")
        return len1, len2

    def insert_data_into_Qh_table(self, res_Qh):
        header = res_Qh[0]
        res_Qh_ = res_Qh[1:]
        if res_Qh_ is not None:
            for row in res_Qh_:
                # CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val is not None:
                        nullrow = False
                        break
                if nullrow:
                    continue
                temp = []
                for val in row:
                    temp.append(str(val))
                ins = (tuple(temp))
                if len(res_Qh_) == 1 and len(res_Qh_[0]) == 1:
                    self.insert_into_r_h_values(header, ins[0])
                else:
                    self.insert_into_r_h_values(header, ins)
