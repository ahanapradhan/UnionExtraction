from abc import ABC

from .AppExtractorBase import AppExtractorBase
from ..executables.nullfree_executable import NullFreeExecutable


def mid_ctid_calculate_shortcut(size):
    mid_row = int(size / 2)
    mid_ctid1 = "(" + str(0) + "," + str(mid_row) + ")"
    mid_ctid2 = "(" + str(0) + "," + str(mid_row + 1) + ")"
    return mid_ctid1, mid_ctid2


class Minimizer(AppExtractorBase, ABC):

    def __init__(self, connectionHelper, core_relations, all_sizes, name):
        AppExtractorBase.__init__(self, connectionHelper, name, all_sizes)
        self.core_relations = core_relations
        self.mock = False
        self.all_relations = list(all_sizes.keys())
        self.all_relations.sort()

    def getCoreSizes(self):
        sizes = {}
        for table in self.all_relations:
            sizes[table] = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(table)))
        self.logger.debug(sizes)
        return sizes

    def extract_params_from_args(self, args):
        return args[0]

    def sanity_check(self, query):
        # SANITY CHECK
        new_result = self.app.doJob(query)
        if self.app.isQ_result_no_full_nullfree_row(new_result):
            self.logger.error("Error: Query out of extractable domain\n")
            return False
        return True

    def create_view_execute_app_drop_view(self,
                                          end_ctid,
                                          mid_ctid1,
                                          mid_ctid2,
                                          query,
                                          start_ctid,
                                          tabname,
                                          dirty_tab):
        if isinstance(self.app, NullFreeExecutable):
            end_ctid, start_ctid = self.check_sanity_when_nullfree_exe(end_ctid, mid_ctid1, mid_ctid2, query,
                                                                       start_ctid,
                                                                       tabname, dirty_tab)
        else:
            end_ctid, start_ctid = self.check_sanity_when_base_exe(end_ctid, mid_ctid1, mid_ctid2, query,
                                                                   start_ctid,
                                                                   tabname, dirty_tab)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(
                                                    self.get_fully_qualified_table_name(tabname))])
        return end_ctid, start_ctid

    def check_sanity_when_nullfree_exe(self, end_ctid, mid_ctid1, mid_ctid2, query, start_ctid, tabname, dirty_tab):
        if self.check_result_for_half(mid_ctid2, end_ctid, dirty_tab, tabname, query):
            # Take the lower half
            start_ctid = mid_ctid2
        elif self.check_result_for_half(start_ctid, mid_ctid1, dirty_tab, tabname, query):
            # Take the upper half
            end_ctid = mid_ctid1
        else:
            self.logger.error("Cannot halve anymore..")
            start_ctid, end_ctid = None, None
        return end_ctid, start_ctid

    def check_sanity_when_base_exe(self, end_ctid, mid_ctid1, mid_ctid2, query, start_ctid, tabname, dirty_tab):
        if self.check_result_for_half(mid_ctid2, end_ctid, dirty_tab, tabname, query):
            # Take the lower half
            start_ctid = mid_ctid2
        else:
            # Take the upper half
            end_ctid = mid_ctid1
        return end_ctid, start_ctid

    def get_start_and_end_ctids(self, core_sizes, query, tabname, dirty_tab):
        self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(
                                                self.get_fully_qualified_table_name(tabname), dirty_tab)])
        end_ctid, mid_ctid1, mid_ctid2, start_ctid = self.get_mid_ctids(core_sizes, tabname, dirty_tab)

        if mid_ctid1 is None:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(
                                        self.get_fully_qualified_table_name(dirty_tab), tabname)])
            return None, None

        self.logger.debug(start_ctid, mid_ctid1, mid_ctid2, end_ctid)
        end_ctid, start_ctid = self.create_view_execute_app_drop_view(end_ctid,
                                                                      mid_ctid1,
                                                                      mid_ctid2,
                                                                      query,
                                                                      start_ctid,
                                                                      tabname,
                                                                      dirty_tab)
        return end_ctid, start_ctid

    def get_mid_ctids(self, core_sizes, tabname, dirty_tab):
        start_page, start_row = self.get_boundary("min", dirty_tab)
        end_page, end_row = self.get_boundary("max", dirty_tab)
        start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
        end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"
        mid_ctid1, mid_ctid2 = mid_ctid_calculate_shortcut(core_sizes[tabname])
        if start_ctid == mid_ctid1:
            mid_ctid1, mid_ctid2 = self.determine_mid_ctid_from_db(dirty_tab)
        return end_ctid, mid_ctid1, mid_ctid2, start_ctid

    def get_boundary(self, min_or_max, tabname):
        m_ctid = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.get_ctid_from(min_or_max, self.get_fully_qualified_table_name(tabname)))
        m_ctid = m_ctid[1:-1]
        m_ctid2 = m_ctid.split(",")
        row = int(m_ctid2[1])
        page = int(m_ctid2[0])
        return page, row

    def check_result_for_half(self, start_ctid, end_ctid, tab, view, query):
        self.connectionHelper.execute_sql(
            [self.connectionHelper.queries.create_view_as_select_star_where_ctid(end_ctid, start_ctid,
                        self.get_fully_qualified_table_name(view), self.get_fully_qualified_table_name(tab))],
            self.logger)
        new_result = self.app.doJob(query)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_view(
                                                    self.get_fully_qualified_table_name(view))], self.logger)
        if self.app.isQ_result_nonEmpty_nullfree(new_result):
            return True  # this half works
        return False  # this half does not work

    def update_with_remaining_size(self, core_sizes, end_ctid, start_ctid, tabname, dirty_tab):
        self.logger.debug(start_ctid, end_ctid)
        self.connectionHelper.execute_sql(
            [self.connectionHelper.queries.create_table_as_select_star_from_ctid(end_ctid, start_ctid,
                                                            self.get_fully_qualified_table_name(tabname),
                                                                 self.get_fully_qualified_table_name(dirty_tab)),
             self.connectionHelper.queries.drop_table_cascade(self.get_fully_qualified_table_name(dirty_tab))],
                                                                    self.logger)
        core_sizes[tabname] = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.get_row_count(self.get_fully_qualified_table_name(tabname)), self.logger)
        self.logger.debug("REMAINING TABLE SIZE", core_sizes[tabname])
        if core_sizes[tabname] == 1:
            res, _ = self.connectionHelper.execute_sql_fetchall(
                                            self.connectionHelper.queries.get_star(
                                                self.get_fully_qualified_table_name(tabname)))
            self.logger.debug(res)
        return core_sizes

    def determine_mid_ctid_from_db(self, tabname):
        count = self.connectionHelper.execute_sql_fetchone_0(
                                            self.connectionHelper.queries.get_row_count(
                                                    self.get_fully_qualified_table_name(tabname)))
        mid_idx = int(count / 2)
        if not mid_idx:
            return None, None
        offset = str(mid_idx - 1)
        mid_ctid1 = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.select_ctid_from_tabname_offset(
                                                self.get_fully_qualified_table_name(tabname), offset))
        self.logger.debug(mid_ctid1)
        mid_ctid2 = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.select_next_ctid(self.get_fully_qualified_table_name(tabname), mid_ctid1))
        self.logger.debug(mid_ctid2)
        return mid_ctid1, mid_ctid2

    def doJob(self, *args):
        super().doJob(*args)
        return self.result

    def see_d_min(self, table):
        res, des = self.connectionHelper.execute_sql_fetchall(self.connectionHelper.queries.get_star(
                                                    self.get_fully_qualified_table_name(table)))
        for row in res:
            self.logger.debug(row)

    def restore_d_min(self):
        for tab in self.core_relations:
            drop_fn = self.get_drop_fn(tab)
            self.connectionHelper.execute_sql([self.connectionHelper.queries.create_table_as_select_star_from(
                self.get_fully_qualified_table_name(self.connectionHelper.queries.get_dmin_tabname(tab)),
                                                        self.get_fully_qualified_table_name(tab)),
                drop_fn(tab), self.connectionHelper.queries.alter_table_rename_to(
                    self.get_fully_qualified_table_name(self.connectionHelper.queries.get_dmin_tabname(tab)), tab)])
