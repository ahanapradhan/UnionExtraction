import ast

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .abstract.MinimizerBase import Minimizer
from .util.common_queries import get_restore_name, drop_table, get_star, get_tabname_nep, alter_view_rename_to, \
    create_table_as_select_star_from
from .util.utils import get_dummy_val_for, get_format, get_char
from ..src.pipeline.abstract.Comparator import Comparator
from ..src.util.utils import get_header_from_cursour_desc


class NepComparator(Comparator):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "NEP Result Comparator", False)

    def match_comparison_based(self, Q_h, Q_E):
        count_star_Q_E = self.create_view_from_Q_E(Q_E)
        self.logger.debug(count_star_Q_E)
        """
        if not count_star_Q_E:
            return False
        """
        self.create_table_from_Qh(Q_h)
        """
        if count_star_Q_E < self.smaller_match_threshold:
            check = self.check_smaller_match(Q_E, res_Qh)
            if not check:
                return False
        """
        check = self.run_except_query_match_and_dropViews()
        return check

    def check_smaller_match(self, Q_E, res_Qh):
        # Drop the temporary table and view created.
        self.connectionHelper.execute_sql(["drop view temp1;"])
        res_Q_E = self.app.doJob(Q_E)
        res_Qh_ = res_Qh
        for i in range(len(res_Q_E)):
            flag = False
            temp = ()
            for ele in (res_Q_E[i]):
                ele = str(ele)
                temp += (ele,)
            self.logger.debug("res_Q_E", temp)
            for j in range(len(res_Qh)):
                self.logger.debug(res_Qh_[j])
                if temp == res_Qh_[j]:
                    res_Qh_[j] = []
                    flag = True
                    break
            if not flag:
                return False
        return True

    def insert_data_into_Qh_table(self, res_Qh):
        # Filling the table temp2
        header = res_Qh[0]
        for i in range(1, len(res_Qh)):
            self.insert_into_temp2_values(header, res_Qh[i])


class NEP(Minimizer, GenerationPipeLineBase):

    def __init__(self, connectionHelper, core_relations, all_sizes,
                 global_pk_dict,
                 global_all_attribs,
                 global_attrib_types,
                 filter_predicates,
                 global_key_attributes,
                 query_generator):
        Minimizer.__init__(self, connectionHelper, core_relations, all_sizes, "NEP")
        GenerationPipeLineBase.__init__(self, connectionHelper, "NEP",
                                        core_relations,
                                        global_all_attribs,
                                        global_attrib_types,
                                        None,
                                        filter_predicates)
        self.filter_attrib_dict = {}
        self.attrib_types_dict = {}
        self.Q_E = ""
        self.global_pk_dict = global_pk_dict  # from initialization
        self.global_key_attributes = global_key_attributes
        self.query_generator = query_generator
        self.nep_comparator = NepComparator(self.connectionHelper)

    def extract_params_from_args(self, args):
        return args[0], args[1]

    def doActualJob(self, args):
        query, Q_E = self.extract_params_from_args(args)
        core_sizes = self.getCoreSizes()

        # STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
        partition_dict = {}
        for key in core_sizes.keys():
            partition_dict[key] = (0, core_sizes[key])

        self.attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}

        self.filter_attrib_dict = self.construct_filter_attribs_dict()

        self.create_all_views()

        # Run the hidden query on the original database instance
        matched = self.nep_comparator.check_matching(query, Q_E)
        if matched:
            nep_exists = False
            self.Q_E = Q_E
            self.logger.info("NEP doesn't exists under our assumptions")
        else:
            self.logger.info("NEP may exists")
            while not matched:
                matched = self.extract_one_nep(Q_E, core_sizes, matched, partition_dict, query)
            nep_exists = True

        self.drop_all_views()
        return nep_exists

    def extract_one_nep(self, Q_E, core_sizes, matched, partition_dict, query):
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            self.Q_E = self.nep_db_minimizer(matched, query, tabname, Q_E, core_sizes[tabname],
                                             partition_dict[tabname], i)
            matched = self.nep_comparator.check_matching(query, self.Q_E)
            self.logger.debug(matched)
            self.logger.debug(self.Q_E)
        return matched

    def create_all_views(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql([drop_table(tabname),
                                               "create view " + tabname + " as select * from "
                                               + get_restore_name(tabname) + " ;"])
        self.logger.info("all views created.")

    def drop_all_views(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(["alter view " + tabname + " rename to " + tabname + "3;",
                                               "create table " + tabname + " as select * from " + tabname + "3;",
                                               "drop view " + tabname + "3 CASCADE;"])
        self.logger.info("all views dropped.")

    def nep_db_minimizer(self, matched, query, tabname, Q_E, tab_size, partition_dict, i):
        """
        Base Case
        """
        if tab_size == 1 and not matched:
            val = self.extract_NEP_value(query, tabname)
            self.logger.debug("NEP val", val)
            if val:
                self.logger.info("Extracting NEP value")
                return self.query_generator.updateExtractedQueryWithNEPVal(query, val)
            else:
                self.logger.debug(Q_E)
                return Q_E

        """
        Drop the current view of name tabname
        Make a view of name x with first half  T <- T_u
        """
        self.connectionHelper.execute_sql(["drop view " + tabname + " CASCADE;"])
        offset, limit = self.create_view_with_upper_half(partition_dict, tabname)
        """
        Run the hidden query on this updated database instance with table T_u
        """
        matched = self.nep_comparator.check_matching(query, Q_E)
        self.logger.debug(matched)
        if not matched:
            Q_E_ = self.nep_db_minimizer(matched, query, tabname, Q_E, limit, (offset, limit), i)
            self.logger.debug(Q_E_)
            return Q_E_
        else:
            Q_E_ = Q_E

        """
        Drop the view of name tabname
        Make a view of name x with second half  T <- T_l
        """
        self.connectionHelper.execute_sql(["drop view " + tabname + " CASCADE;"])
        offset, limit = self.create_view_with_lower_half(partition_dict, tabname)
        """
        Run the hidden query on this updated database instance with table T_l
        """
        matched = self.nep_comparator.check_matching(query, Q_E_)
        self.logger.debug(matched)
        if not matched:
            Q_E__ = self.nep_db_minimizer(matched, query, tabname, Q_E_, limit, (offset, limit), i)
            self.logger.debug(Q_E__)
            return Q_E__
        else:
            return Q_E_

    def create_view_with_upper_half(self, partition_dict, tabname):
        offset = int(partition_dict[0])
        limit = int(partition_dict[1] / 2)
        self.create_view_from_offset_limit(limit, offset, tabname)
        return offset, limit

    def create_view_with_lower_half(self, partition_dict, tabname):
        offset = int(partition_dict[0]) + int(partition_dict[1] / 2)
        limit = int(partition_dict[1]) - int(partition_dict[1] / 2)
        self.create_view_from_offset_limit(limit, offset, tabname)
        return offset, limit

    def create_view_from_offset_limit(self, limit, offset, tabname):
        self.logger.debug("table", tabname, "offset ", offset, " limit ", limit)
        self.connectionHelper.execute_sql(["create view " + tabname + " as select * from " + get_restore_name(
            tabname) + " order by " + self.global_pk_dict[tabname] + " offset " + str(offset) \
                                           + " limit " + str(limit) + ";"])

    def extract_NEP_value(self, query, tabname):
        # Return if hidden executable is giving non-empty output on the reduced database
        # It means that the current table does not contain NEP source column
        new_result = self.app.doJob(query)
        if len(new_result) > 1:
            return False

        # convert the view into a table
        self.connectionHelper.execute_sql([alter_view_rename_to(tabname, get_tabname_nep(tabname)),
                                           create_table_as_select_star_from(tabname, get_tabname_nep(tabname))])

        # check nep for every non-key attribute by changing its value to different s value and run the executable.
        # If the output came out non- empty. It means that nep is present on that attribute with previous value.
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            attrib_list = self.global_all_attribs[i]
            filterAttribs = []
            filterAttribs = self.check_per_attrib(attrib_list,
                                                  tabname,
                                                  query,
                                                  filterAttribs)
            if filterAttribs is not None and len(filterAttribs):
                return filterAttribs

        # convert the table back to view
        self.connectionHelper.execute_sql([drop_table(tabname),
                                           alter_view_rename_to(get_tabname_nep(tabname), tabname)])
        return False

    def check_per_attrib(self, attrib_list, tabname, query, filterAttribs):
        for attrib in attrib_list:
            if attrib not in self.global_key_attributes:
                val = self.get_val(attrib, tabname)

                prev = self.connectionHelper.execute_sql_fetchone_0("SELECT " + attrib + " FROM " + tabname + ";")

                self.update_with_val(attrib, tabname, val)
                self.logger.debug("update ", tabname, attrib, "with value ", val)

                new_result = self.app.doJob(query)

                if len(new_result) > 1:
                    filterAttribs.append((tabname, attrib, '<>', prev))
                    self.logger.debug(filterAttribs, '++++++_______++++++')
                    # convert the table back to view
                    self.connectionHelper.execute_sql([drop_table(tabname),
                                                       alter_view_rename_to(get_tabname_nep(tabname), tabname)])
                    return filterAttribs

    def update_with_val(self, attrib, tabname, val):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = "UPDATE " + tabname + " SET " + attrib + " = " + val + ";"
        elif 'int' in self.attrib_types_dict[(tabname, attrib)] or 'numeric' in self.attrib_types_dict[
            (tabname, attrib)]:
            update_q = "UPDATE " + tabname + " SET " + attrib + " = " + str(val) + ";"
        else:
            update_q = "UPDATE " + tabname + " SET " + attrib + " = '" + val + "';"
        self.connectionHelper.execute_sql([update_q])

    def get_val(self, attrib, tabname):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for('date')
            val = ast.literal_eval(get_format('date',val))

        elif ('int' in self.attrib_types_dict[(tabname, attrib)] or 'numeric' in self.attrib_types_dict[
            (tabname, attrib)]):
            # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for('int')
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = (self.filter_attrib_dict[(tabname, attrib)].replace('%', ''))
            else:
                val = get_char(get_dummy_val_for('char'))
        return val
