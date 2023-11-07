import pandas as pd

from ..util.common_queries import create_view_as_select_star_where_ctid, drop_view, alter_table_rename_to, \
    get_ctid_from, create_table_as_select_star_from_ctid, drop_table, get_row_count, get_star, \
    create_table_as_select_star_from, get_tabname_4, select_ctid_from_tabname_offset, select_next_ctid
from ..util.utils import isQ_result_empty

from ...refactored.abstract.ExtractorBase import Base
from ...refactored.executable import Executable


class Minimizer(Base):

    def __init__(self, connectionHelper, core_relations, all_sizes, name):
        Base.__init__(self, connectionHelper, name)
        self.core_relations = core_relations
        self.app = Executable(connectionHelper)
        self.all_sizes = all_sizes
        self.global_min_instance_dict = {}
        self.mock = False

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        minimized = self.do_minimizeJob(query)

        if minimized:
            self.create_d_min_db()

            if not self.sanity_check(query):
                return False

            self.populate_min_instance_dict()
            return True
        return False

    def getCoreSizes(self):
        core_sizes = {}
        for table in self.core_relations:
            core_sizes[table] = self.all_sizes[table]
        return core_sizes

    def extract_params_from_args(self, args):
        return args[0]

    def sanity_check(self, query):
        # SANITY CHECK
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
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
                                          tabname1):
        if not self.check_result_for_half(mid_ctid2, end_ctid, tabname1, tabname, query):
            # Take the upper half
            end_ctid = mid_ctid1
        else:
            # Take the lower half
            start_ctid = mid_ctid2
        self.connectionHelper.execute_sql([drop_view(tabname)])
        return end_ctid, start_ctid

    def calculate_mid_ctids(self, start_page, end_page, size):
        mid_row = int(size / 2)
        mid_ctid1 = "(" + str(0) + "," + str(mid_row) + ")"
        mid_ctid2 = "(" + str(0) + "," + str(mid_row + 1) + ")"
        return mid_ctid1, mid_ctid2

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

    def get_mid_ctids(self, core_sizes, tabname, tabname1):
        start_page, start_row = self.get_boundary("min", tabname1)
        end_page, end_row = self.get_boundary("max", tabname1)
        start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
        end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"
        mid_ctid1, mid_ctid2 = self.calculate_mid_ctids(start_page, end_page, core_sizes[tabname])
        if start_ctid == mid_ctid1:
            mid_ctid1, mid_ctid2 = self.determine_mid_ctid_from_db(tabname1)
        return end_ctid, mid_ctid1, mid_ctid2, start_ctid

    def get_boundary(self, min_or_max, tabname):
        m_ctid = self.connectionHelper.execute_sql_fetchone_0(get_ctid_from(min_or_max, tabname))
        m_ctid = m_ctid[1:-1]
        m_ctid2 = m_ctid.split(",")
        row = int(m_ctid2[1])
        page = int(m_ctid2[0])
        return page, row

    def check_result_for_half(self, start_ctid, end_ctid, tab, view, query):
        self.connectionHelper.execute_sql(
            [create_view_as_select_star_where_ctid(end_ctid, start_ctid, view, tab)])
        new_result = self.app.doJob(query)
        self.connectionHelper.execute_sql([drop_view(view)])
        if not isQ_result_empty(new_result):
            return True  # this half works
        return False  # this half does not work

    def update_with_remaining_size(self, core_sizes, end_ctid, start_ctid, tabname, tabname1):
        self.logger.debug(start_ctid, end_ctid)
        self.connectionHelper.execute_sql(
            [create_table_as_select_star_from_ctid(end_ctid, start_ctid, tabname, tabname1),
             drop_table(tabname1)])
        core_sizes[tabname] = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
        self.logger.debug("REMAINING TABLE SIZE", core_sizes[tabname])
        return core_sizes

    def determine_mid_ctid_from_db(self, tabname):
        count = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
        mid_idx = int(count / 2)
        if not mid_idx:
            return None, None
        offset = str(mid_idx - 1)
        mid_ctid1 = self.connectionHelper.execute_sql_fetchone_0(select_ctid_from_tabname_offset(tabname, offset))
        self.logger.debug(mid_ctid1)
        mid_ctid2 = self.connectionHelper.execute_sql_fetchone_0(select_next_ctid(tabname, mid_ctid1))
        self.logger.debug(mid_ctid2)
        return mid_ctid1, mid_ctid2

    def doJob(self, *args):
        super().doJob(*args)
        '''
        if self.mock:
            self.restore_d_min()
            self.see_d_min()
        '''
        return self.result

    def see_d_min(self):
        for tab in self.core_relations:
            res, des = self.connectionHelper.execute_sql_fetchall(get_star(tab))
            for row in res:
                self.logger.debug(row)

    def restore_d_min(self):
        for tab in self.core_relations:
            drop_fn = self.get_drop_fn(tab)
            self.connectionHelper.execute_sql([create_table_as_select_star_from(get_tabname_4(tab), tab),
                                               drop_fn(tab), alter_table_rename_to(get_tabname_4(tab), tab)])

    def populate_min_instance_dict(self):
        # POPULATE MIN INSTANCE DICT
        for tabname in self.core_relations:
            self.global_min_instance_dict[tabname] = []
            sql_query = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            self.global_min_instance_dict[tabname].append(tuple(df.columns))
            for index, row in df.iterrows():
                self.global_min_instance_dict[tabname].append(tuple(row))

    def create_d_min_db(self):
        for tabname in self.core_relations:
            self.connectionHelper.execute_sql([drop_table(get_tabname_4(tabname)),
                                               create_table_as_select_star_from(get_tabname_4(tabname), tabname)])
            res, desc = self.connectionHelper.execute_sql_fetchall(get_star(tabname))
            self.logger.debug(tabname, "==", res)

    def do_minimizeJob(self, query):
        return True



