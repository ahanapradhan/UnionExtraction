from .util.common_queries import hashtext_query
from ..src.pipeline.abstract.Comparator import Comparator


class ResultComparator(Comparator):

    def __init__(self, connectionHelper, isHash):
        super().__init__(connectionHelper, "Result Comparator", True)
        self.isHash = isHash

    def is_match(self, len1, len2):
        if self.isHash:
            if len1 == len2:
                return True
            else:
                return False
        else:
            return super().is_match(len1, len2)

    def run_diff_queries(self):
        if self.isHash:
            return self.run_hash_diff_queries()
        return super().run_diff_queries()

    def run_hash_diff_queries(self):
        len1 = self.connectionHelper.execute_sql_fetchone_0(hashtext_query(self.r_e))
        len2 = self.connectionHelper.execute_sql_fetchone_0(hashtext_query(self.r_h))
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
                # print(ins)
                if len(res_Qh_) == 1 and len(res_Qh_[0]) == 1:
                    self.insert_into_r_h_values(header, ins[0])
                else:
                    self.insert_into_r_h_values(header, ins)
