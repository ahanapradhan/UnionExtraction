import copy

import pandas as pd

from .abstract.MinimizerBase import Minimizer
from ..refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_1, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_4, get_star, \
    get_restore_name
from ..refactored.util.utils import isQ_result_empty


def extract_start_and_end_page(rctid):
    min_ctid = rctid[0]
    min_ctid2 = min_ctid.split(",")
    start_page = int(min_ctid2[0][1:])
    max_ctid = rctid[1]
    # print(max_ctid)
    max_ctid2 = max_ctid.split(",")
    end_page = int(max_ctid2[0][1:])
    start_ctid = min_ctid
    end_ctid = max_ctid
    return end_ctid, end_page, start_ctid, start_page


class ViewMinimizer(Minimizer):
    max_row_no = 1

    def __init__(self, connectionHelper,
                 core_relations, core_sizes,
                 sampling_status):
        super().__init__(connectionHelper, core_relations, core_sizes, "View_Minimizer")
        self.cs2_passed = sampling_status

        self.global_other_info_dict = {}
        self.global_result_dict = {}
        self.local_other_info_dict = {}
        self.global_min_instance_dict = {}

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        return self.reduce_Database_Instance(query,
                                             True) if self.cs2_passed else self.reduce_Database_Instance(query, False)

    def do_binary_halving(self, core_sizes,
                          query,
                          tabname,
                          rctid,
                          tabname1):
        end_ctid, end_page, start_ctid, start_page = extract_start_and_end_page(rctid)
        while start_page < end_page - 1:
            mid_page = int((start_page + end_page) / 2)
            mid_ctid1 = "(" + str(mid_page) + ",1)"
            mid_ctid2 = "(" + str(mid_page) + ",2)"

            end_ctid, start_ctid = self.create_view_execute_app_drop_view(end_ctid, mid_ctid1, mid_ctid2, query,
                                                                          start_ctid, tabname, tabname1)
            start_ctid2 = start_ctid.split(",")
            start_page = int(start_ctid2[0][1:])
            end_ctid2 = end_ctid.split(",")
            end_page = int(end_ctid2[0][1:])

        core_sizes = self.update_with_remaining_size(core_sizes, end_ctid, start_ctid, tabname, tabname1)
        return core_sizes

    def update_with_remaining_size(self, core_sizes, end_ctid, start_ctid, tabname, tabname1):
        self.connectionHelper.execute_sql(
            [create_table_as_select_star_from_ctid(end_ctid, start_ctid, tabname, tabname1),
             drop_table(tabname1)])
        size = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tabname))
        core_sizes[tabname] = size
        # print("REMAINING TABLE SIZE", core_sizes[tabname])
        return core_sizes

    def create_view_execute_app_drop_view(self, end_ctid, mid_ctid1, mid_ctid2, query, start_ctid, tabname, tabname1):
        self.connectionHelper.execute_sql(
            [create_view_as_select_star_where_ctid(mid_ctid1, start_ctid, tabname, tabname1)])
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            # Take the lower half
            start_ctid = mid_ctid2
        else:
            # Take the upper half
            end_ctid = mid_ctid1
        self.connectionHelper.execute_sql([drop_view(tabname)])
        return end_ctid, start_ctid

    def sanity_check(self, query):
        # SANITY CHECK
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            print("Error: Query out of extractable domain\n")
            return False
        return True

    def reduce_Database_Instance(self, query, cs_pass):

        self.local_other_info_dict = {}
        core_sizes = self.getCoreSizes()

        for tabname in self.core_relations:
            view_name = get_tabname_1(tabname) if cs_pass else get_restore_name(tabname)
            self.connectionHelper.execute_sql([alter_table_rename_to(tabname, view_name)])
            rctid = self.connectionHelper.execute_sql_fetchone(get_min_max_ctid(view_name))
            core_sizes = self.do_binary_halving(core_sizes, query, tabname, rctid, view_name)
            core_sizes = self.do_binary_halving_1(core_sizes, query, tabname, get_tabname_1(tabname))

            if not self.sanity_check(query):
                return False

        for tabname in self.core_relations:
            res, desc = self.connectionHelper.execute_sql_fetchall(get_star(tabname))
            self.connectionHelper.execute_sql([drop_table(get_tabname_4(tabname)),
                                               create_table_as_select_star_from(get_tabname_4(tabname), tabname)])
            # print(tabname, "==", res)

        if not self.sanity_check(query):
            return False

        self.populate_dict_info(query)
        return True

    def do_binary_halving_1(self, core_sizes, query, tabname, tabname1):
        while int(core_sizes[tabname]) > self.max_row_no:
            self.connectionHelper.execute_sql([alter_table_rename_to(tabname, tabname1)])
            start_page, start_row = self.get_boundary("min", tabname)
            end_page, end_row = self.get_boundary("max", tabname)

            start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
            end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"
            mid_row = int(core_sizes[tabname] / 2)
            mid_ctid1 = "(" + str(0) + "," + str(mid_row) + ")"
            mid_ctid2 = "(" + str(0) + "," + str(mid_row + 1) + ")"

            end_ctid, start_ctid = self.create_view_execute_app_drop_view(end_ctid, mid_ctid1, mid_ctid2, query,
                                                                          start_ctid, tabname, tabname1)
            core_sizes = self.update_with_remaining_size(core_sizes, end_ctid, start_ctid, tabname, tabname1)

        return core_sizes

    def populate_dict_info(self, query):
        # POPULATE MIN INSTANCE DICT
        for tabname in self.core_relations:
            self.global_min_instance_dict[tabname] = []
            sql_query = pd.read_sql_query(get_star(tabname), self.connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            self.global_min_instance_dict[tabname].append(tuple(df.columns))
            for index, row in df.iterrows():
                self.global_min_instance_dict[tabname].append(tuple(row))

        # populate other data
        new_result = self.app.doJob(query)
        self.global_result_dict['min'] = copy.deepcopy(new_result)
        self.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
        self.global_other_info_dict['min'] = copy.deepcopy(self.local_other_info_dict)

    def get_boundary(self, min_or_max, tabname):
        m_ctid = self.connectionHelper.execute_sql_fetchone_0(
            get_ctid_from(min_or_max, get_tabname_1(tabname)))
        # max_ctid = max_ctid[0]
        m_ctid = m_ctid[1:-1]
        m_ctid2 = m_ctid.split(",")
        row = int(m_ctid2[1])
        page = int(m_ctid2[0])
        return page, row
