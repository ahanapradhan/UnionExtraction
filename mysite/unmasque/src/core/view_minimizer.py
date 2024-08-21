import pandas as pd

from .abstract.MinimizerBase import Minimizer


def extract_start_and_end_page(logger, rctid):
    min_ctid = rctid[0]
    min_ctid2 = min_ctid.split(",")
    start_page = int(min_ctid2[0][1:])
    max_ctid = rctid[1]
    logger.debug(max_ctid)
    max_ctid2 = max_ctid.split(",")
    end_page = int(max_ctid2[0][1:])
    start_ctid = min_ctid
    end_ctid = max_ctid
    return end_ctid, end_page, start_ctid, start_page


class ViewMinimizer(Minimizer):
    max_row_no = 1

    def __init__(self, connectionHelper,
                 core_relations, all_sizes,
                 sampling_status):
        super().__init__(connectionHelper, core_relations, all_sizes, "View_Minimizer")
        self.cs2_passed = sampling_status
        self.global_min_instance_dict = {}

    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        if not self.sanity_check(query):
            self.logger.error(" Original database is not giving populated result!")
            return False
        return self.reduce_Database_Instance(query,
                                             True) if self.cs2_passed else self.reduce_Database_Instance(query, False)

    def do_viewBased_binary_halving(self, core_sizes,
                                    query,
                                    tabname,
                                    rctid,
                                    tabname1):
        end_ctid, end_page, start_ctid, start_page = extract_start_and_end_page(self.logger, rctid)
        while start_page < end_page - 1:
            mid_page = int((start_page + end_page) / 2)
            mid_ctid1 = "(" + str(mid_page) + ",1)"
            mid_ctid2 = "(" + str(mid_page) + ",2)"

            end_ctid, start_ctid = self.create_view_execute_app_drop_view(end_ctid,
                                                                          mid_ctid1, mid_ctid2, query,
                                                                          start_ctid, tabname, tabname1)
            if end_ctid is None:
                return core_sizes
            start_ctid2 = start_ctid.split(",")
            start_page = int(start_ctid2[0][1:])
            end_ctid2 = end_ctid.split(",")
            end_page = int(end_ctid2[0][1:])

        core_sizes = self.update_with_remaining_size(core_sizes, end_ctid, start_ctid, tabname, tabname1)
        return core_sizes

    def reduce_Database_Instance(self, query, cs_pass):
        core_sizes = self.getCoreSizes()

        for tabname in self.core_relations:
            view_name = self.connectionHelper.queries.get_tabname_1(
                tabname) if cs_pass else self.connectionHelper.queries.get_restore_name(tabname)
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(tabname, view_name)])
            rctid = self.connectionHelper.execute_sql_fetchone(
                self.connectionHelper.queries.get_min_max_ctid(view_name))
            core_sizes = self.do_viewBased_binary_halving(core_sizes, query, tabname, rctid, view_name)
            core_sizes = self.do_copyBased_binary_halving(core_sizes, query, tabname,
                                                          self.connectionHelper.queries.get_tabname_1(tabname))

            if not self.sanity_check(query):
                return False

        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(
                [self.connectionHelper.queries.drop_table_cascade(self.connectionHelper.queries.get_tabname_4(tabname)),
                 self.connectionHelper.queries.create_table_as_select_star_from(
                     self.connectionHelper.queries.get_tabname_4(tabname), tabname)])

        self.populate_dict_info()
        return True

    def reduce_Database_Instance_kapil(self, query, cs_pass):
        core_sizes = self.getCoreSizes()

        for tabname in self.core_relations:
            view_name = self.connectionHelper.queries.get_tabname_1(
                tabname) if cs_pass else self.connectionHelper.queries.get_restore_name(tabname)
            core_sizes = self.do_copyBased_binary_halving(core_sizes, query, tabname, view_name)

            if not self.sanity_check(query):
                return False

        for tabname in self.core_relations:
            self.connectionHelper.execute_sql(
                [self.connectionHelper.queries.drop_table_cascade(self.connectionHelper.queries.get_tabname_4(tabname)),
                 self.connectionHelper.queries.create_table_as_select_star_from(
                     self.connectionHelper.queries.get_tabname_4(tabname), tabname)])

        self.populate_dict_info()
        return True

    def do_copyBased_binary_halving(self, core_sizes, query, tabname, tabname1):
        while int(core_sizes[tabname]) > self.max_row_no:
            end_ctid, start_ctid = self.get_start_and_end_ctids(core_sizes, query, tabname, tabname1)
            core_sizes = self.update_with_remaining_size(core_sizes, end_ctid, start_ctid, tabname, tabname1)
        return core_sizes

    def populate_dict_info(self):
        # POPULATE MIN INSTANCE DICT
        for tabname in self.core_relations:
            self.global_min_instance_dict[tabname] = []
            sql_query = pd.read_sql_query(self.connectionHelper.queries.get_star(tabname), self.connectionHelper.conn)
            df = pd.DataFrame(sql_query)
            self.global_min_instance_dict[tabname].append(tuple(df.columns))
            for index, row in df.iterrows():
                self.global_min_instance_dict[tabname].append(tuple(row))
