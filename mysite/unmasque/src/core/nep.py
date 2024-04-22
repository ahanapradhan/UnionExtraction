import copy
from typing import List

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .abstract.MinimizerBase import Minimizer
from .result_comparator import ResultComparator
from ..util.utils import isQ_result_empty


class NepComparator(ResultComparator):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, True)
        self.earlyExit = False


class NepMinimizer(Minimizer):

    def __init__(self, connectionHelper, core_relations, all_sizes={}):
        super().__init__(connectionHelper, core_relations, all_sizes, "NEP Minimizer")
        self.Q_E = None
        self.nep_comparator = NepComparator(self.connectionHelper)

    def set_all_relations(self, relations: list[str]):
        self.all_relations = copy.deepcopy(relations)
        self.nep_comparator.set_all_relations(self.all_relations)

    def extract_params_from_args(self, args):
        return args[0][0], args[0][1], args[0][2]

    def doActualJob(self, args=None):
        query, self.Q_E, table = self.extract_params_from_args(args)
        return self.reduce_Database_Instance(query, table)

    def reduce_Database_Instance(self, query, table):
        check = self.nep_comparator.db_restorer.restore_table_and_confirm(table)
        if not check:
            self.logger.error("Error while restoring table. Aborting NEP minimization!")
            return False
        self.getCoreSizes()
        self.logger.debug("Inside get nep")
        tabname1 = self.connectionHelper.queries.get_tabname_1(table)
        while self.all_sizes[table] > 1:
            self.logger.debug("Inside minimization loop")
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(table, tabname1)],
                                              self.logger)
            end_ctid, start_ctid = self.get_start_and_end_ctids(self.all_sizes, query, table, tabname1)
            self.logger.debug(end_ctid, start_ctid)
            if end_ctid is None:
                self.connectionHelper.execute_sql(
                    [self.connectionHelper.queries.alter_table_rename_to(tabname1, table)], self.logger)
                return False  # no role on NEP
            self.all_sizes = self.update_with_remaining_size(self.all_sizes, end_ctid, start_ctid, table, tabname1)
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
        if self.check_result_for_half(mid_ctid2, end_ctid, tabname1, tabname, query):
            # Take the lower half
            start_ctid = mid_ctid2
        elif self.check_result_for_half(start_ctid, mid_ctid1, tabname1, tabname, query):
            # Take the upper half
            end_ctid = mid_ctid1
        else:
            self.logger.error("None of the halves could find out the differentiating tuple! Something is wrong!")
            return None, None
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
        if self.nep_comparator.row_count_r_e > 1 and self.nep_comparator.row_count_r_h <= self.nep_comparator.row_count_r_e:
            return True
        elif not self.nep_comparator.row_count_r_e:
            return False
        return True

    def match(self, query, Q_E):
        return self.nep_comparator.doJob(query, Q_E)


class NEP(GenerationPipeLineBase):
    loop_count_cutoff = 10
    '''
    NEP extractor do not terminate if input Q_E is not correct. This cutoff is to prevent infinite looping
    It imposes on the user the following restriction: hidden query cannot have more than 10 NEPs.
    Of course we can set this cutoff to much higher value if needed.
    '''

    def __init__(self, connectionHelper, core_relations, query_generator, delivery, all_sizes={}):
        super().__init__(connectionHelper, delivery)
        self.nep_minimizer = NepMinimizer(self.connectionHelper, core_relations, all_sizes)
        self.filter_attrib_dict = {}
        self.attrib_types_dict = {}
        self.Q_E = ""
        self.query_generator = query_generator
        self.wrong = False
        self.enabled = self.connectionHelper.config.detect_nep

    def set_all_relations(self, relations: list[str]):
        super().set_all_relations(relations)
        self.nep_minimizer.set_all_relations(relations)

    def extract_params_from_args(self, args):
        return args[0][0], args[0][1]

    def do_one_round_nep(self, query, nep_exists, matched, is_for_joined):
        loop_count = 0
        while not matched and loop_count < self.loop_count_cutoff:
            loop_count += 1
            for i, tabname in enumerate(self.core_relations):
                self.logger.debug(f"loop count {loop_count}")
                self.logger.info("NEP may exists")
                nep_exists = True
                minimized = self.nep_minimizer.doJob(query, self.Q_E, tabname)
                if not minimized:
                    continue
                Q_E = self.get_nep(tabname, query, i, is_for_joined)
                if Q_E is None:
                    self.logger.error("Q_E is None. Cannot do NEP extraction!")
                    self.wrong = True
                    break
                self.Q_E = Q_E
                matched = self.nep_minimizer.match(query, self.Q_E)
                if matched:
                    break
        return nep_exists, matched

    def doActualJob(self, args=None):
        query, Q_E = self.extract_params_from_args(args)
        nep_exists = False
        # Run the hidden query on the original database instance
        matched = self.nep_comparator.doJob(query, Q_E)
        if matched is None:
            self.logger.error("Extracted Query is not semantically correct!..not going to try to extract NEP!")
            return False

        self.Q_E = Q_E
        nep_exists, matched = self.do_one_round_nep(query, nep_exists, matched, False)
        if matched:
            return nep_exists
        if self.wrong:
            return False
        nep_exists, matched = self.do_one_round_nep(query, nep_exists, matched, True)
        if matched:
            return nep_exists

    def get_nep(self, tabname, query, i, is_for_joined):
        self.logger.debug("Inside get nep")
        res = self.app.doJob(query)
        if isQ_result_empty(res):
            attrib_list = self.global_all_attribs[i]
            self.logger.debug("attrib list: ", attrib_list)
            filterAttribs = []
            filterAttribs = self.check_per_attrib(attrib_list,
                                                  tabname,
                                                  query,
                                                  filterAttribs, is_for_joined)
            if filterAttribs is not None and len(filterAttribs):
                return self.query_generator.updateExtractedQueryWithNEPVal(query, filterAttribs)
        return self.Q_E

    def check_per_attrib(self, attrib_list, tabname, query, filterAttribs, is_for_joined):
        if is_for_joined:
            self.check_per_joined_attrib(attrib_list, filterAttribs, query, tabname)
        else:
            self.check_per_single_attrib(attrib_list, filterAttribs, query, tabname)
        return filterAttribs

    def check_per_joined_attrib(self, attrib_list, filterAttribs, query, tabname):
        if self.joined_attribs is None:
            return
        joined_attribs = [attrib for attrib in attrib_list if attrib in self.joined_attribs]
        for attrib in joined_attribs:
            join_tabnames = []
            other_attribs = self.get_other_attribs_in_eqJoin_grp(attrib)
            val, prev = self.update_attrib_to_see_impact(attrib, tabname)
            self.update_attribs_bulk(join_tabnames, other_attribs, val)
            new_result = self.app.doJob(query)
            self.update_with_val(attrib, tabname, prev)
            self.update_attribs_bulk(join_tabnames, other_attribs, prev)
            self.update_filter_attribs_from_res(new_result, filterAttribs, tabname, attrib, prev)

    def check_per_single_attrib(self, attrib_list, filterAttribs, query, tabname):
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
            self.update_filter_attribs_from_res(new_result, filterAttribs, tabname, attrib, prev)

    def update_filter_attribs_from_res(self, new_result, filterAttribs, tabname, attrib, prev):
        if not isQ_result_empty(new_result):
            filterAttribs.append((tabname, attrib, '<>', prev))
            self.logger.debug(filterAttribs, '++++++_______++++++')
