from ...refactored.abstract.MinimizerBase import Minimizer
from ...refactored.util.common_queries import alter_table_rename_to, get_tabname_1, drop_view


def is_ctid_less_than(x, end_ctid):
    ex = x[1:-1]
    ec = end_ctid[1:-1]
    exes = ex.split(',')
    eces = ec.split(',')
    return int(exes[0]) < int(eces[0]) and int(exes[1]) < int(eces[1])


def is_ctid_greater_than(x, end_ctid):
    ex = x[1:-1]
    ec = end_ctid[1:-1]
    exes = ex.split(',')
    eces = ec.split(',')
    return (int(exes[0]) == int(eces[0]) and int(exes[1]) > int(eces[1])) or (int(exes[0]) > int(eces[0]))


def get_ctid_one_less(ctid):
    po = ctid[1:-1]
    idx = po.split(',')
    if int(idx[1]) > 1:
        less_one = str(int(idx[1]) - 1)
        return f"({idx[0], less_one})"
    return None


def format_ctid(ctid):
    ctid = ctid[1:-1]
    ctid = ctid.replace("\'", "")
    return ctid


class NMinimizer(Minimizer):
    SWTICH_TRANSACTION_ON = 2000

    def __init__(self, connectionHelper,
                 core_relations, core_sizes):
        super().__init__(connectionHelper, core_relations, core_sizes, "N_Minimizer")
        self.core_sizes = core_sizes
        self.may_exclude = {}
        self.must_include = {}
        self.view_drop_count = 0

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        for tab in self.core_relations:
            self.minimize_one_relation1(query, tab)
        return True

    def minimize_one_relation1(self, query, tab):
        sctid = '(0,1)'
        ectid = self.connectionHelper.execute_sql_fetchone_0(f"Select MAX(ctid) from {tab};")
        end_ctid, start_ctid = self.try_binary_halving(query, tab)
        self.core_sizes = self.update_with_remaining_size(self.core_sizes,
                                                          end_ctid,
                                                          start_ctid, tab, get_tabname_1(tab))
        while sctid != start_ctid or ectid != end_ctid:
            sctid = start_ctid
            ectid = end_ctid
            end_ctid, start_ctid = self.try_binary_halving(query, tab)
            if end_ctid is None:
                return
            self.core_sizes = self.update_with_remaining_size(self.core_sizes,
                                                              end_ctid,
                                                              start_ctid, tab, get_tabname_1(tab))

        self.may_exclude[tab] = []  # set of tuples that can be removed for getting non empty result
        self.must_include[tab] = []  # set of tuples that must be preserved for getting non empty result

        self.logger.debug(start_ctid, end_ctid)

        # first time

        self.may_exclude[tab].append(end_ctid)

        c2tid = self.connectionHelper.execute_sql_fetchone_0(f"SELECT MAX(ctid) FROM {tab} WHERE ctid < '{end_ctid}';")
        self.logger.debug(c2tid)
        self.must_include[tab].append(c2tid)

        self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_1(tab))])
        size = self.core_sizes[tab]
        while True:
            self.connectionHelper.execute_sql([drop_view(tab)])
            self.swicth_transaction(tab)
            ok = self.is_ok_without_tuple1(tab, query, start_ctid)
            if ok == "DONE":
                break
            self.logger.debug("may exclude", self.may_exclude[tab])
            self.logger.debug("must include", self.must_include[tab])
            if size == self.core_sizes[tab]:
                break

        self.core_sizes[tab] = len(self.must_include[tab])
        self.logger.debug(self.core_sizes[tab])

    def is_ok_without_tuple1(self, tab, query, start_ctid):
        end_ctid = self.must_include[tab][-1]

        if is_ctid_greater_than(start_ctid, end_ctid):
            self.must_include[tab].pop()
            return "DONE"

        exclude_ctids = ""
        for x in self.may_exclude[tab]:
            if is_ctid_less_than(x, end_ctid):
                exclude_ctids += f" and ctid != '{x})'"

        include_ctids = ""
        for x in self.must_include[tab]:
            if is_ctid_greater_than(x, end_ctid):
                include_ctids += f" or ctid = '{x}'"

        create_cmd = f"Create view {tab} as Select * From {get_tabname_1(tab)} " \
                     f"Where ctid >= '{start_ctid}' and ctid <= '{end_ctid}' {exclude_ctids} {include_ctids};"
        self.logger.debug(create_cmd)
        self.connectionHelper.execute_sql([create_cmd])
        if self.sanity_check(query):
            self.core_sizes[tab] -= 1
            self.may_exclude[tab].pop()
            self.may_exclude[tab].append(self.must_include[tab].pop())
            self.must_include[tab].append(self.get_previous_ctid(end_ctid, tab))
            return True
        else:
            self.must_include[tab].insert(0, self.may_exclude[tab].pop())
            self.may_exclude[tab].append(self.get_previous_ctid(end_ctid, tab))
        return False

    def get_previous_ctid(self, end_ctid, tab):
        nctid = get_ctid_one_less(end_ctid)
        if nctid is None:
            nctid = self.connectionHelper.execute_sql_fetchone_0(
                f"Select MAX(ctid) from {get_tabname_1(tab)} Where ctid < '{end_ctid}';")
        else:
            nctid = format_ctid(nctid)
        return nctid

    '''
    Database has a limit on maximum number of locks per transaction.
    Experimentally it was found ~2500+ for Postgres running locally.
    too many object creation/drop causes acquiring lock too many times, causing shared memory to go out of limit.
    Hence, switching transaction.
    '''

    def swicth_transaction(self, tab):
        self.view_drop_count += 1
        self.logger.debug(self.view_drop_count)
        if self.view_drop_count >= self.SWTICH_TRANSACTION_ON:
            self.connectionHelper.closeConnection()
            self.connectionHelper.connectUsingParams()
            self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_1(tab))])
            self.view_drop_count = 0

    def try_binary_halving(self, query, tab):
        return self.get_start_and_end_ctids(self.core_sizes, query, tab, get_tabname_1(tab))

    def calculate_mid_ctids(self, start_page, end_page, size):
        mid_page = int((start_page + end_page) / 2)
        mid_ctid1 = "(" + str(mid_page) + ",1)"
        mid_ctid2 = "(" + str(mid_page) + ",2)"
        return mid_ctid1, mid_ctid2

    def create_view_execute_app_drop_view(self,
                                          end_ctid,
                                          mid_ctid1,
                                          mid_ctid2,
                                          query,
                                          start_ctid,
                                          tabname,
                                          tabname1):
        if self.check_result_for_half(start_ctid, mid_ctid1, tabname1, tabname, query):
            # Take the upper half
            end_ctid = mid_ctid1
        elif self.check_result_for_half(mid_ctid2, end_ctid, tabname1, tabname, query):
            # Take the lower half
            start_ctid = mid_ctid2
        self.connectionHelper.execute_sql([drop_view(tabname)])
        return end_ctid, start_ctid
