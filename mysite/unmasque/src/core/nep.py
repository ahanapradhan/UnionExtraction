from typing import List

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from ..util.constants import NUMBER_TYPES
from .abstract.MinimizerBase import Minimizer
from .filter import Filter
from .result_comparator import ResultComparator


class NepComparator(ResultComparator):
    def __init__(self, connectionHelper, core_relations):
        super().__init__(connectionHelper, True, core_relations)
        self.earlyExit = False
        self.re_tab = "re_E"
        self.use_re = self.connectionHelper.config.detect_oj

    def create_view_from_Q_E(self, Q_E):
        try:
            self.logger.debug(Q_E)
            # Run the extracted query Q_E .
            self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(self.r_e),
                                               self.connectionHelper.queries.create_view_as(self.r_e, Q_E)],
                                              self.logger)

            if self.use_re:
                self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table_cascade(self.re_tab),
                                                   f"Create unlogged table {self.re_tab} (like {self.r_e});"],
                                                  self.logger)
                r_E = self.app.doJob(Q_E)
                self.insert_data_into_Qh_table(r_E, self.re_tab)
                self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(self.r_e),
                                                   self.connectionHelper.queries.create_view_as(self.r_e,
                                                                                                f"select * from {self.re_tab};")],
                                                  self.logger)
        except ValueError as e:
            self.logger.error(e)
            return False

        # Size of the table
        res = self.connectionHelper.execute_sql_fetchone_0(self.connectionHelper.queries.get_row_count(self.r_e))
        return res

    def run_diff_query_match_and_dropViews(self):
        check = super().run_diff_query_match_and_dropViews()
        if self.use_re:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table_cascade(self.re_tab)],
                                              self.logger)
        return check

    def insert_data_into_Qh_table(self, res_Qh, table):
        header = res_Qh[0]
        res_Qh_ = res_Qh[1:]
        if res_Qh_ not in [None, 'None']:
            for row in res_Qh_:
                if any(val in [None, 'None'] for val in row):
                    continue
                ins = (tuple(row))
                if len(res_Qh_) == 1 and len(res_Qh_[0]) == 1:
                    self.insert_into_result_table_values(header, ins[0], table)
                else:
                    self.insert_into_result_table_values(header, ins, table)


class NepMinimizer(Minimizer):

    def __init__(self, connectionHelper, core_relations, all_sizes):
        super().__init__(connectionHelper, core_relations, all_sizes, "NEP Minimizer")
        self.Q_E = None
        self.nep_comparator = NepComparator(self.connectionHelper, core_relations)

    def sanity_check(self, query):
        result_e = self.app.doJob(self.Q_E)
        result_h = self.app.doJob(query)
        if self.app.isQ_result_no_full_nullfree_row(result_h) and self.app.isQ_result_nonEmpty_nullfree(result_e):
            return True
        return False

    def set_all_relations(self, relations: List[str]):
        super().set_all_relations(relations)
        self.nep_comparator.set_all_relations(self.all_relations)

    def extract_params_from_args(self, args):
        return args[0][0], args[0][1], args[0][2]

    def doActualJob(self, args=None):
        query, self.Q_E, table = self.extract_params_from_args(args)
        try:
            self.app.doJob(self.Q_E)
        except:
            self.logger.error("Q_E is not semantically correct.")
            return False
        return self.reduce_Database_Instance(query, table)

    def reduce_Database_Instance(self, query, table):
        core_sizes = self.getCoreSizes()
        self.logger.debug("Inside get nep")
        tabname1 = self.connectionHelper.queries.get_tabname_1(table)
        while core_sizes[table] > 1:
            self.logger.debug("Inside minimization loop")
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(table, tabname1)],
                                              self.logger)
            end_ctid, start_ctid = self.get_start_and_end_ctids(core_sizes, query, table, tabname1)
            self.logger.debug(end_ctid, start_ctid)
            if end_ctid is None:
                self.connectionHelper.execute_sql(
                    [self.connectionHelper.queries.alter_table_rename_to(tabname1, table)], self.logger)
                return False  # no role on NEP
            core_sizes = self.update_with_remaining_size(core_sizes, end_ctid, start_ctid, table, tabname1)
        return True

    def get_mid_ctids(self, core_sizes, tabname, tabname1):
        start_page, start_row = self.get_boundary("min", tabname1)
        end_page, end_row = self.get_boundary("max", tabname1)
        start_ctid = f"({str(start_page)},{str(start_row)})"
        end_ctid = f"({str(end_page)},{str(end_row)})"
        mid_ctid1, mid_ctid2 = self.determine_mid_ctid_from_db(tabname1)
        return end_ctid, mid_ctid1, mid_ctid2, start_ctid

    def get_start_and_end_ctids(self, core_sizes, query, tabname, tabname1):
        end_ctid, mid_ctid1, mid_ctid2, start_ctid = self.get_mid_ctids(core_sizes, tabname, tabname1)

        if mid_ctid1 is None:
            return None, None

        self.logger.debug(start_ctid, mid_ctid1, mid_ctid2, end_ctid)
        end_ctid, start_ctid = self.create_view_execute_app_drop_view(end_ctid,
                                                                      mid_ctid1,
                                                                      mid_ctid2,
                                                                      query,
                                                                      start_ctid,
                                                                      tabname,
                                                                      tabname1)
        return end_ctid, start_ctid

    def create_view_execute_app_drop_view(self,
                                          end_ctid,
                                          mid_ctid1,
                                          mid_ctid2,
                                          query,
                                          start_ctid,
                                          tabname,
                                          tabname1):
        end_ctid, start_ctid = self.check_sanity_when_nullfree_exe(end_ctid, mid_ctid1, mid_ctid2, query,
                                                                   start_ctid,
                                                                   tabname, tabname1)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(tabname)])
        return end_ctid, start_ctid

    def check_result_for_half(self, start_ctid, end_ctid, tab, view, query):
        self.logger.debug("view: ", view, " from table ", tab)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(view),
                                           self.connectionHelper.queries.create_view_as_select_star_where_ctid(end_ctid,
                                                                                                               start_ctid,
                                                                                                               view,
                                                                                                               tab)])

        self.logger.debug(start_ctid, end_ctid)
        found = self.nep_comparator.match(query, self.Q_E)
        if found:
            return False
        self.logger.debug(self.nep_comparator.row_count_r_e, self.nep_comparator.row_count_r_h)
        if self.nep_comparator.row_count_r_e == 1 and not self.nep_comparator.row_count_r_h:
            return True
        if self.nep_comparator.row_count_r_e > 1 \
                and self.nep_comparator.row_count_r_h < self.nep_comparator.row_count_r_e:
            return True
        elif not self.nep_comparator.row_count_r_e:
            return False
        return True

    def match(self, query, Q_E):
        return self.nep_comparator.doJob(query, Q_E)[0]


class NEP(GenerationPipeLineBase):

    def __init__(self, connectionHelper, genCtx):
        super().__init__(connectionHelper, "NEP Extractor", genCtx)
        self.filter_extractor = Filter(self.connectionHelper, self.core_relations, self.global_min_instance_dict)
        self.filter_extractor.do_init()
        self.get_datatype = self.filter_extractor.get_datatype

    def extract_params_from_args(self, args):
        return args[0][0], args[0][1]

    def extract_NEP_for_table(self, tabname, query, is_for_joined):
        self.logger.debug("Inside get nep")
        res = self.app.doJob(query)
        filterAttribs = []
        if self.app.isQ_result_empty(res):
            attrib_list = self.global_all_attribs[tabname]
            self.logger.debug("attrib list: ", attrib_list)
            filterAttribs = self.__check_per_attrib(attrib_list,
                                                    tabname,
                                                    query,
                                                    filterAttribs, is_for_joined)
        return filterAttribs

    def doActualJob(self, args=None):
        query, table = self.extract_params_from_args(args)
        nonKey_filter = self.extract_NEP_for_table(table, query, False)
        key_filter = self.extract_NEP_for_table(table, query, True)
        return nonKey_filter + key_filter

    def __check_per_attrib(self, attrib_list, tabname, query, filterAttribs, is_for_joined):
        if is_for_joined:
            self.__check_per_joined_attrib(attrib_list, filterAttribs, query, tabname)
        else:
            self.__check_per_single_attrib(attrib_list, filterAttribs, query, tabname)
        return filterAttribs

    def __update_key_attib_with_val(self, tabs, attribs, value, prev):
        for tup in list(zip(tabs, attribs)):
            datatype = self.get_datatype(tup)
            qoted = False if datatype in NUMBER_TYPES else True
            self.connectionHelper.execute_sql([self.connectionHelper.queries.update_key_attrib_with_val(tup[0], tup[1], value, prev, qoted)])

    def __check_per_joined_attrib(self, attrib_list, filterAttribs, query, tabname):
        if self.joined_attribs is None:
            return
        joined_attribs = [attrib for attrib in attrib_list if attrib in self.joined_attribs]
        skip_attribs = []
        for attrib in joined_attribs:
            if attrib in skip_attribs:
                continue
            other_attribs = self.get_other_attribs_in_eqJoin_grp(attrib)
            skip_attribs.extend(other_attribs)
            join_tabnames = [self.find_tabname_for_given_attrib(attrb) for attrb in other_attribs]
            val, prev = self.update_attrib_to_see_impact(attrib, tabname)
            self.__update_key_attib_with_val(join_tabnames, other_attribs, val, prev)
            # self.see_d_min()
            new_result = self.app.doJob(query)
            other_attribs.append(attrib)
            join_tabnames.append(tabname)
            self.__update_key_attib_with_val(join_tabnames, other_attribs, prev, val)
            self.__update_filter_attribs_from_res(new_result, filterAttribs, join_tabnames, other_attribs, prev, query)

    def __check_per_single_attrib(self, attrib_list, filterAttribs, query, tabname):
        if self.joined_attribs is not None:
            single_attribs = [attrib for attrib in attrib_list if attrib not in self.joined_attribs]
        else:
            single_attribs = attrib_list
        for attrib in single_attribs:
            self.logger.debug(tabname, attrib)
            prev = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.select_attribs_from_relation([attrib], tabname))
            val = self.get_different_s_val(attrib, tabname, prev)
            self.logger.debug("update ", tabname, attrib, "with value ", val, " prev", prev)
            self.update_with_val(attrib, tabname, val)
            new_result = self.app.doJob(query)
            self.update_with_val(attrib, tabname, prev)
            self.__update_filter_attribs_from_res(new_result, filterAttribs, [tabname], [attrib], prev, query)

    def __update_filter_attribs_from_res(self, new_result, filterAttribs, tabs, attribs, prev, query):
        if not self.app.isQ_result_empty(new_result):
            filterAttribs.append((tabs[-1], attribs[-1], '<>', prev))
            self.logger.debug(filterAttribs, '++++++_______++++++')