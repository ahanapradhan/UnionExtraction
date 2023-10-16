import ast

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .abstract.MinimizerBase import Minimizer
from .result_comparator import ResultComparator

from .util.common_queries import get_restore_name, drop_table, get_tabname_nep, alter_view_rename_to, \
    create_table_as_select_star_from, alter_table_rename_to, drop_view, create_view_as_select_star_where_ctid, \
    get_tabname_1
from .util.utils import get_dummy_val_for, get_format, get_char, isQ_result_empty


class NepComparator(ResultComparator):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "NEP Result Comparator")
        self.earlyExit = False



class NEP(Minimizer, GenerationPipeLineBase):

    def __init__(self, connectionHelper, core_relations, all_sizes, global_pk_dict, global_all_attribs,
                 global_attrib_types, filter_predicates, global_key_attributes, query_generator,
                 global_min_instance_dict):
        Minimizer.__init__(self, connectionHelper, core_relations, all_sizes, "NEP")
        GenerationPipeLineBase.__init__(self, connectionHelper, "NEP", core_relations, global_all_attribs,
                                        global_attrib_types, None, filter_predicates, global_min_instance_dict)
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

        # self.create_all_views()

        # Run the hidden query on the original database instance

        matched = self.nep_comparator.match(query, Q_E)

        if matched:
            nep_exists = False
            self.Q_E = Q_E
            self.logger.info("NEP doesn't exists under our assumptions")
        else:
            self.logger.info("NEP may exists")
            while not matched:

                matched = self.extract_one_nep(Q_E, core_sizes, matched, partition_dict, query)

            nep_exists = True

        self.drop_all_views1()
        return nep_exists

    def extract_one_nep(self, Q_E, core_sizes, matched, partition_dict, query):
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            self.Q_E = Q_E
            self.Q_E = self.get_nep(core_sizes, tabname, query, i)
            # self.Q_E = self.nep_db_minimizer(matched, query, tabname, Q_E, core_sizes[tabname], partition_dict[tabname], i)
            matched = self.nep_comparator.match(query, self.Q_E)
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


    def drop_all_views1(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql([drop_view(tabname), create_table_as_select_star_from(tabname,
                                                                                                    get_restore_name(tabname))])
        self.logger.info("all views dropped.")

    def get_nep(self, core_sizes, tabname, query, i):
        tabname1 = get_tabname_1(tabname)
        end_ctid, start_ctid = self.get_start_and_end_ctids(core_sizes, query, tabname, tabname1)
        if end_ctid is None:
            return  # no role on NEP
        self.connectionHelper.execute_sql([create_view_as_select_star_where_ctid(end_ctid, start_ctid, tabname, tabname1)])
        val = self.get_NEP_val(query, tabname, i)
        if val:
            self.logger.info("Extracting NEP value")
            return self.query_generator.updateExtractedQueryWithNEPVal(query, val)

    def get_start_and_end_ctids(self, core_sizes, query, tabname, tabname1):
        self.connectionHelper.execute_sql([alter_table_rename_to(tabname, tabname1)])
        end_ctid, mid_ctid1, mid_ctid2, start_ctid = self.get_mid_ctids(core_sizes, tabname, tabname1)

        if mid_ctid1 is None:
            self.connectionHelper.execute_sql([alter_table_rename_to(tabname1, tabname)])
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

    def check_result_for_half(self, start_ctid, end_ctid, tab, view, query):
        self.connectionHelper.execute_sql(
            [create_view_as_select_star_where_ctid(end_ctid, start_ctid, view, tab)])
        matched = self.nep_comparator.match(query, self.Q_E)
        self.logger.debug(matched)
        self.connectionHelper.execute_sql([drop_view(view)])
        return not matched  # if both queries produce different result, this is it!

    def nep_db_minimizer(self, matched, query, tabname, Q_E, tab_size, partition_dict, i):
        """
        Base Case
        """
        if tab_size == 1 and not matched:
            val = self.get_NEP_val(query, tabname, i)

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
        matched = self.nep_comparator.match(query, Q_E)
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
        matched = self.nep_comparator.match(query, Q_E_)
        self.logger.debug(matched)
        if not matched:
            Q_E__ = self.nep_db_minimizer(matched, query, tabname, Q_E_, limit, (offset, limit), i)
            self.logger.debug(Q_E__)
            return Q_E__
        else:
            return Q_E_


    def get_NEP_val(self, query, tabname, i):
        # convert the view into a table
        self.connectionHelper.execute_sql([alter_view_rename_to(tabname, get_tabname_nep(tabname)),
                                           create_table_as_select_star_from(tabname, get_tabname_nep(tabname))])
        self.logger.debug(tabname, " is now a table")

        val = self.extract_NEP_value(query, tabname, i)
        self.logger.debug("NEP val", val)

        # convert the table back to view
        self.connectionHelper.execute_sql([drop_table(tabname),
                                           alter_view_rename_to(get_tabname_nep(tabname), tabname)])
        self.logger.debug(tabname, " is now a view")
        return val

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


    def extract_NEP_value(self, query, tabname, i):
        # Return if hidden executable is giving non-empty output on the reduced database
        # It means that the current table does not contain NEP source column
        new_result = self.app.doJob(query)
        if len(new_result) > 1:
            return False

        # check nep for every non-key attribute by changing its value to different s value and run the executable.
        # If the output came out non- empty. It means that nep is present on that attribute with previous value.
        # for i in range(len(self.core_relations)):
        #    tabname = self.core_relations[i]
        attrib_list = self.global_all_attribs[i]
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
                val = self.get_val(attrib, tabname)

                prev = self.connectionHelper.execute_sql_fetchone_0("SELECT " + attrib + " FROM " + tabname + ";")
                self.logger.debug("update ", tabname, attrib, "with value ", val, " prev", prev)
                self.update_with_val(attrib, tabname, val)

                new_result = self.app.doJob(query)

                if len(new_result) > 1:
                    filterAttribs.append((tabname, attrib, '<>', prev))
                    self.logger.debug(filterAttribs, '++++++_______++++++')
                    return filterAttribs

    def update_with_val(self, attrib, tabname, val):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = "UPDATE " + tabname + " SET " + attrib + " = " + get_format('date', val) + ";"
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
            val = ast.literal_eval(get_format('date', val))

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

