from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .abstract.MinimizerBase import Minimizer
from .result_comparator import ResultComparator
from .util.common_queries import alter_table_rename_to, create_view_as_select_star_where_ctid, \
    get_tabname_1, drop_view, drop_table, get_restore_name, create_table_as_select_star_from


class NepComparator(ResultComparator):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "NEP Result Comparator")
        self.earlyExit = False


class NEP(Minimizer, GenerationPipeLineBase):
    loop_count_cutoff = 10
    '''
    NEP extractor do not terminate if input Q_E is not correct. This cutoff is to prevent infinite looping
    It imposes on the user the following restriction: hidden query cannot have more than 10 NEPs.
    Of course we can set this cutoff to much higher value if needed.
    '''

    def __init__(self, connectionHelper, core_relations, all_sizes, global_pk_dict, global_all_attribs,
                 global_attrib_types, filter_predicates, global_key_attributes, query_generator,
                 global_min_instance_dict):
        Minimizer.__init__(self, connectionHelper, core_relations, all_sizes, "NEP")
        GenerationPipeLineBase.__init__(self, connectionHelper, "NEP", core_relations, global_all_attribs,
                                        global_attrib_types, None, filter_predicates,
                                        global_min_instance_dict, global_key_attributes)
        self.filter_attrib_dict = {}
        self.attrib_types_dict = {}
        self.Q_E = ""
        self.global_pk_dict = global_pk_dict  # from initialization
        self.query_generator = query_generator

        self.nep_comparator = NepComparator(self.connectionHelper)

    def extract_params_from_args(self, args):
        return args[0][0], args[0][1]

    def doActualJob(self, args):
        query, Q_E = self.extract_params_from_args(args)

        self.attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        self.filter_attrib_dict = self.construct_filter_attribs_dict()
        nep_exists = False

        # Run the hidden query on the original database instance
        matched = self.nep_comparator.doJob(query, Q_E)
        if matched is None:
            self.logger.error("Extracted Query is not semantically correct!..not going to try to extract NEP!")
            return False

        self.Q_E = Q_E
        loop_count = 0
        while not matched and loop_count < self.loop_count_cutoff:
            i = 0
            for tabname in self.core_relations:
                loop_count += 1
                self.logger.debug(f"loop count {loop_count}")

                # tabname = self.core_relations[i]
                self.logger.info("NEP may exists")
                nep_exists = True
                self.backup_relation(tabname)

                core_sizes = self.getCoreSizes()
                self.Q_E = self.get_nep(core_sizes, tabname, query, i)

                i += 1

                if self.Q_E is None:
                    self.logger.error("Something is wrong")
                    return False
                matched = self.nep_comparator.doJob(query, self.Q_E)
                if matched:
                    break

        return nep_exists

    def restore_relation(self, table):
        self.connectionHelper.execute_sql([drop_table(table),
                                           alter_table_rename_to(get_restore_name(table), table)])

    def backup_relation(self, table):
        self.connectionHelper.execute_sql([drop_table(get_restore_name(table)),
                                           create_table_as_select_star_from(
                                               get_restore_name(table), table)])

    def get_nep(self, core_sizes, tabname, query, i):
        tabname1 = get_tabname_1(tabname)
        while core_sizes[tabname] > 1:
            self.connectionHelper.execute_sql([alter_table_rename_to(tabname, tabname1)])
            end_ctid, start_ctid = self.get_start_and_end_ctids(core_sizes, query, tabname, tabname1)
            if end_ctid is None:
                self.connectionHelper.execute_sql([alter_table_rename_to(tabname1, tabname)])
                return  # no role on NEP
            core_sizes = self.update_with_remaining_size(core_sizes, end_ctid, start_ctid, tabname, tabname1)

        # self.see_d_min()

        val = self.extract_NEP_value(query, tabname, i)
        if val:
            self.logger.info("Extracting NEP value")
            return self.query_generator.updateExtractedQueryWithNEPVal(query, val)
        else:
            return self.Q_E

    def get_mid_ctids(self, core_sizes, tabname, tabname1):
        start_page, start_row = self.get_boundary("min", tabname1)
        end_page, end_row = self.get_boundary("max", tabname1)
        start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
        end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"
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
            self.logger.error("something is wrong!")
            return None, None
        self.connectionHelper.execute_sql([drop_view(tabname)])
        return end_ctid, start_ctid

    def check_result_for_half(self, start_ctid, end_ctid, tab, view, query):
        self.connectionHelper.execute_sql([drop_view(view),
                                           create_view_as_select_star_where_ctid(end_ctid, start_ctid, view, tab)])

        found = self.nep_comparator.match(query, self.Q_E)
        if found:
            return False
        query = query.replace(";", "")
        q_e = self.Q_E.replace(";", "")
        query_result = self.connectionHelper.execute_sql_fetchone_0(f"select count(*) from ({query}) as q_h;")
        q_e_result = self.connectionHelper.execute_sql_fetchone_0(f"select count(*) from ({q_e}) as q_e;")
        self.logger.debug(f"q_e result: {q_e_result}, query result: {query_result}")
        '''
        if q_e_result >= 1 and query_result >= 1:
            return True
        elif q_e_result == 1 and query_result == 0:
            return True
        elif q_e_result == 0 and query_result == 1:
            return False
        elif q_e_result == 0 and query_result == 0:
            return False
        '''
        if q_e_result >= 1:
            return True
        elif not q_e_result:
            return False

    def extract_NEP_value(self, query, tabname, i):
        self.logger.debug("extract NEP val ", tabname, i)
        res = self.app.doJob(query)
        if len(res) > 1:
            return False
        attrib_list = self.global_all_attribs[i]
        self.logger.debug("attrib list: ", attrib_list)
        filterAttribs = []
        filterAttribs = self.check_per_attrib(attrib_list,
                                              tabname,
                                              query,
                                              filterAttribs)
        if filterAttribs is not None and len(filterAttribs):
            return filterAttribs
        return False

    def check_per_attrib(self, attrib_list, tabname, query, filterAttribs):
        for attrib in attrib_list:
            self.logger.debug(tabname, attrib)
            if attrib not in self.global_key_attributes:

                prev = self.connectionHelper.execute_sql_fetchone_0(f"SELECT {attrib} FROM {tabname};")
                val = self.get_different_val(attrib, tabname, prev)
                self.logger.debug("update ", tabname, attrib, "with value ", val, " prev", prev)
                self.update_with_val(attrib, tabname, val)

                new_result = self.app.doJob(query)

                if len(new_result) > 1:
                    filterAttribs.append((tabname, attrib, '<>', prev))
                    self.logger.debug(filterAttribs, '++++++_______++++++')
                    return filterAttribs
