from mysite.unmasque.src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase
from mysite.unmasque.src.core.result_comparator import ResultComparator
from mysite.unmasque.src.util.utils import get_min_and_max_val


class HiddenAggregate(GenerationPipeLineBase):

    def __init__(self, connectionHelper, delivery, global_pk_dict):
        super().__init__(connectionHelper, "Nested Aggregated", delivery)
        self.comparator = ResultComparator(self.connectionHelper, True, delivery.core_relations, False)
        self.global_pk_dict = global_pk_dict
        self.inner_query = ""

    def __assertEqAtSingleRowDmin(self, q_h, q_e) -> bool:
        self.do_init()
        check = self.comparator.doJob(q_h, q_e)
        return check

    def __check_for_sum(self, tab, attrib, prev, limit, datatype, query, Q_E) -> bool:
        check_list = []
        unit = (limit - 1) / 2
        self.update_with_val(attrib, tab, unit)
        self.logger.debug(f"update {tab}.{attrib} with val {unit}")
        res = self.app.doJob(query)
        if not self.app.isQ_result_empty(res):
            check_list.append(True)
        else:
            check_list.append(False)
        unit = (limit + 1) / 2
        self.update_with_val(attrib, tab, unit)
        self.logger.debug(f"update {tab}.{attrib} with val {unit}")
        res = self.app.doJob(query)
        if self.app.isQ_result_empty(res):
            check_list.append(True)
        else:
            check_list.append(False)
        self.logger.debug(f"Check list: {check_list}")
        return all(flag for flag in check_list)

    def __check_for_max(self, tab, attrib, prev, limit, datatype, query, Q_E) -> bool:
        _min, _ = get_min_and_max_val(datatype)
        _max = limit + 2
        self.logger.debug(f"max = {_max}, avg = {str((_min + _max) / 2)}")
        # violate max but satisfy avg
        res = self.__update_with_two_distinct_vals_and_doAppExe(_max, _min, attrib, query, tab)
        self.logger.debug(res)
        if self.app.isQ_result_empty(res):
            return True
        return False

    def __update_with_two_distinct_vals_and_doAppExe(self, _max, _min, attrib, query, tab):
        pk = self.global_pk_dict[tab]
        pks, _ = self.connectionHelper.execute_sql_fetchall(
            self.connectionHelper.queries.select_attribs_from_relation([pk], tab))
        self.logger.debug(pks)
        val_where = [(_min, pks[0][0]), (_max, pks[1][0])]
        for vh in val_where:
            self.connectionHelper.execute_sql([f"update {tab} set {attrib} = {vh[0]} where {pk} = {vh[1]};"],
                                              self.logger)
        # self.see_d_min()
        res = self.app.doJob(query)
        return res

    def __check_for_avg(self, tab, attrib, prev, limit, datatype, query, Q_E) -> bool:
        _max = limit + 3
        _min = limit - 1
        self.logger.debug(f"max = {_max}, avg = {str((_min + _max) / 2)}")
        # violate avg but satisfy min
        res = self.__update_with_two_distinct_vals_and_doAppExe(_max, _min, attrib, query, tab)
        if not self.app.isQ_result_empty(res):
            return True
        return False

    def __check_for_min(self, tab, attrib, prev, limit, datatype, query, Q_E) -> bool:
        return not self.__check_for_avg(tab, attrib, prev, limit, datatype, query, Q_E)

    def __filter_candidates(self, tab, query, Q_E):
        check_list = []
        for fl in self.global_filter_predicates:
            table, attrib, op, lb, ub = fl[0], fl[1], fl[2], fl[3], fl[4]
            datatype = self.get_datatype((table, attrib))
            if datatype in ['str', 'date']:
                self.logger.info("Do not support Textual/Date Predicates currently")
                continue
            if table != tab:
                continue
            self.logger.debug(f"{fl} is a possible candidate for hidden aggregation.")
            limit = ub if op == '<=' else lb
            prev = self.get_dmin_val(attrib, table)
            self.logger.debug(f"{tab}.{attrib} = {prev}, limit is {limit} with aggregate function.")
            subquery_select = self.__agg_func_check(Q_E, attrib, check_list, datatype, limit, prev, query, tab)
            self.logger.debug(subquery_select)
            self.inner_query = subquery_select
        return check_list

    def __agg_func_check(self, Q_E, attrib, check_list, datatype, limit, prev, query, tab) -> str:
        func_names = ['SUM', 'MAX', 'AVG', 'MIN']
        funcs = [self.__check_for_sum, self.__check_for_max, self.__check_for_avg, self.__check_for_min]
        local_check_list = []
        for i, fun in enumerate(funcs):
            agg_check = fun(tab, attrib, prev, limit, datatype, query, Q_E)
            self.logger.debug(f"{str(fun)} gave {agg_check}")
            local_check_list.append(agg_check)
            if agg_check:
                for k in range(i+1, len(funcs)):
                    local_check_list.append(False)
                break
        check_list.append(local_check_list.count(True) == 1)
        f_i = local_check_list.index(True)
        inner_q_part = f"Select {func_names[f_i]}({attrib}) from {tab}"
        return inner_q_part

    def doActualJob(self, args=None):
        query, Q_E = self.extract_params_from_args(args)
        check = self.__assertEqAtSingleRowDmin(query, Q_E)
        if check:
            self.logger.info(" It may be a nested aggregate. ")
            for tab in self.core_relations:
                self.__two_rows_table_generation(tab)
                check = self.comparator.doJob(query, Q_E)
                if check:
                    self.__revert_two_rows_generation(tab)
                else:
                    self.see_d_min()
                    if self.comparator.row_count_r_e > self.comparator.row_count_r_h:
                        self.logger.info(" It is a nested aggregate in the WHERE clause. ")
                        check_list = self.__filter_candidates(tab, query, Q_E)
                        return all(flag for flag in check_list)
        return self.inner_query

    def __revert_two_rows_generation(self, tab):
        self.logger.debug("No effect. Reverting changes!")
        self.connectionHelper.execute_sql([self.connectionHelper.queries.truncate_table(tab)])
        self.connectionHelper.execute_sql(
            [f"Insert into {tab} (select * from {self.connectionHelper.queries.get_backup(tab)} limit 1);"])
        self.restore_d_min_from_dict_for_tab(tab)

    def __two_rows_table_generation(self, tab):
        temp_tab = self.connectionHelper.queries.get_tabname_1(tab)
        pk_tab = self.global_pk_dict[tab]
        pk_dmin = self.get_dmin_val(pk_tab, tab)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.create_table_like(temp_tab, tab),
                                           f"Insert into {temp_tab} (select * from {tab} limit 1);",
                                           f"update {temp_tab} set {pk_tab} = {pk_dmin} + 1;",
                                           f"Insert into {tab} (select * from {temp_tab});",
                                           self.connectionHelper.queries.drop_table(temp_tab)])
        rows = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(tab))
        self.logger.debug(f"{rows} rows in table {tab}")

    def extract_params_from_args(self, args):
        query, Q_E = args[0][0], args[0][1]
        self.logger.debug(f"Hidden Query: {query}")
        self.logger.debug(f"Extracted Query: {Q_E}")
        return query, Q_E
