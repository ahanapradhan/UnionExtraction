from ...refactored.abstract.MinimizerBase import Minimizer
from ...refactored.util.common_queries import alter_table_rename_to, get_restore_name, drop_view


class NMinimizer(Minimizer):

    def __init__(self, connectionHelper,
                 core_relations, core_sizes):
        super().__init__(connectionHelper, core_relations, core_sizes, "N_Minimizer")
        self.core_sizes = core_sizes
        self.may_exclude = {}
        self.must_include = {}

    def is_ok_without_tuple(self, tabname, query):
        exclude_ctids = ""
        for x in self.may_exclude[tabname]:
            exclude_ctids += f" and ctid != '(0,{str(x)})'"

        end_ctid_idx = self.must_include[tabname][-1]
        start_ctid = "(0,1)"
        end_ctid = f"(0,{str(end_ctid_idx)})"
        create_cmd = f"Create view {tabname} as Select * From {get_restore_name(tabname)} " \
                     f"Where ctid >= '{start_ctid}' and ctid <= '{end_ctid}' {exclude_ctids} ;"
        self.logger.debug(create_cmd)
        self.connectionHelper.execute_sql([create_cmd])
        self.logger.debug("end_ctid_idx", end_ctid_idx)

        if self.sanity_check(query):
            if len(self.must_include[tabname]) > 1:
                em = self.must_include[tabname][-2]
                self.may_exclude[tabname].append(em)
                self.must_include[tabname].remove(em)
            return True
        else:
            self.logger.debug("end_ctid_idx", end_ctid_idx)
        return False

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        for tab in self.core_relations:
            size = self.core_sizes[tab]
            self.may_exclude[tab] = []  # set of tuples that can be removed for getting non empty result
            self.must_include[tab] = []  # set of tuples that can be removed for getting non empty result

            self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_restore_name(tab))])
            ok = True

            for row in range(self.core_sizes[tab]):
                if ok:
                    self.must_include[tab].append(size - row)
                else:
                    self.may_exclude[tab].append(self.must_include[tab].pop())
                self.connectionHelper.execute_sql([drop_view(tab)])
                ok = self.is_ok_without_tuple(tab, query)
                self.logger.debug(self.may_exclude[tab])
                self.logger.debug(self.must_include[tab])

            self.core_sizes[tab] = len(self.must_include[tab])
        return True
