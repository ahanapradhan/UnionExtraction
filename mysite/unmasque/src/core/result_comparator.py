from ...src.pipeline.abstract.Comparator import Comparator


class ResultComparator(Comparator):

    def __init__(self, connectionHelper, isHash, core_relations=None):
        super().__init__(connectionHelper, "Result_Comparator", True, core_relations)
        self.isHash = isHash

    def is_match(self, len1, len2):
        self._create_working_schema()
        self.logger.debug(f"{len1} len1, {len2} len2")
        if self.isHash:
            if len1 is not None and len1 == len2:
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
        len1 = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.hashtext_query(self.r_e, self.get_fully_qualified_table_name(self.r_e)))
        len2 = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.hashtext_query(self.r_h, self.get_fully_qualified_table_name(self.r_h)))
        return len1, len2

    def insert_data_into_Qh_table(self, res_Qh, table):
        header = res_Qh[0]
        res_Qh_ = res_Qh[1:]
        if res_Qh_ not in [None, 'None']:
            for row in res_Qh_:
                # CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val not in [None, 'None']:
                        nullrow = False
                        break
                if nullrow:
                    continue
                temp = []
                for val in row:
                    if val == 'None':
                        temp.append('NULL')
                    else:
                        temp.append(str(val))
                ins = (tuple(temp))
                if len(res_Qh_) == 1 and len(res_Qh_[0]) == 1:
                    self.insert_into_result_table_values(header, ins[0], table)
                else:
                    self.insert_into_result_table_values(header, ins, table)
