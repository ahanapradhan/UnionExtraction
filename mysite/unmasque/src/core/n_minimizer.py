from _decimal import Decimal
from datetime import date
from itertools import combinations

from ..util.constants import DONE
from ...refactored.abstract.MinimizerBase import Minimizer
from ...refactored.util.common_queries import alter_table_rename_to, get_tabname_1, drop_view, select_previous_ctid, \
    get_row_count, select_start_ctid_of_any_table, drop_table, get_ctid_from


def make_total_ctid_list(end_ctid, mid_ctid_list, start_ctid):
    ctid_list = [start_ctid]
    for mid_ctid in mid_ctid_list:
        ctid_list.append(mid_ctid[0])
        ctid_list.append(mid_ctid[1])
    ctid_list.append(end_ctid)
    return ctid_list


def get_combinations(original_list, choose):
    result_list = [list(combination) for combination in combinations(original_list, choose)]
    print(result_list)
    return result_list


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


def is_ctid_equal(one_ctid, other_ctid):
    one = one_ctid[1:-1]
    other = other_ctid[1:-1]
    one_sp = one.split(',')
    other_sp = other.split(',')
    one_sp_i = [int(x) for x in one_sp]
    other_sp_i = [int(x) for x in other_sp]
    return one_sp_i == other_sp_i


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
        self.max_ary = 4

    def do_minimizeJob(self, query):
        for tab in self.core_relations:
            self.ary = 2
            self.minimize_table(query, tab)
        return True

    def minimize_table(self, query, tab):
        while self.ary < self.max_ary:
            ctid_range = self.do_bulk_minimization_till_possible(query, tab)
            if None in ctid_range:
                break
            self.logger.debug(ctid_range)
            self.ary += 1

        end_ctid = self.connectionHelper.execute_sql_fetchone_0(get_ctid_from("max", tab))
        start_ctid = self.connectionHelper.execute_sql_fetchone_0(get_ctid_from("min", tab))

        if is_ctid_equal(start_ctid, end_ctid):
            return

        self.may_exclude[tab] = []  # set of tuples that can be removed for getting non empty result
        self.must_include[tab] = []  # set of tuples that must be preserved for getting non empty result

        # first time
        self.may_exclude[tab].append(end_ctid)
        c2tid = self.connectionHelper.execute_sql_fetchone_0(select_previous_ctid(tab, end_ctid))
        self.must_include[tab].append(c2tid)

        self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_1(tab))])
        self.do_row_by_row_elimination(query, start_ctid, tab)

        self.connectionHelper.execute_sql([drop_view(tab)])
        self.create_table_with_selected_ctids(tab, get_tabname_1(tab))
        self.connectionHelper.execute_sql([drop_table(get_tabname_1(tab))])

        count = self.connectionHelper.execute_sql_fetchone_0(get_row_count(tab))

        self.core_sizes[tab] = count
        self.logger.debug(self.core_sizes[tab])

    def do_row_by_row_elimination(self, query, start_ctid, tab):
        while True:
            self.connectionHelper.execute_sql([drop_view(tab)])
            self.swicth_transaction(tab)
            ok = self.is_ok_to_eliminate_previous_tuple(tab, query, start_ctid)
            self.logger.debug("may exclude", self.may_exclude[tab])
            self.logger.debug("must include", self.must_include[tab])

            if len(self.may_exclude[tab]):
                if is_ctid_equal(self.may_exclude[tab][0], select_start_ctid_of_any_table()):
                    if not ok:
                        self.must_include[tab].append(self.may_exclude[tab].pop())
                    return DONE
            else:
                return DONE

    def do_bulk_minimization_till_possible(self, query, tab):
        self.logger.debug(f"++++++++****************< {self.ary} Divisions in bulk >**************+++++++++++")
        while True:
            tab_size = self.core_sizes[tab]
            ctid_range, check = self.get_valid_ctid_range(self.core_sizes, query, tab, get_tabname_1(tab))
            if check:
                self.core_sizes = self.update_with_remaining_size(self.core_sizes, ctid_range, tab, get_tabname_1(tab))
                self.logger.debug(tab_size, self.core_sizes[tab])
            if tab_size == self.core_sizes[tab]:  # did not reduce anymore
                break
        return ctid_range

    def create_table_with_selected_ctids(self, tab, fromtab):
        include_ctids = []
        for x in self.must_include[tab]:
            include_ctids.append(f" ctid = '{x}' ")
        include_str = " or ".join(include_ctids)

        create_cmd = f"Create table {tab} as Select * From {fromtab} " \
                     f"Where {include_str};"
        self.connectionHelper.execute_sql([create_cmd])

    def is_ok_to_eliminate_previous_tuple(self, tab, query, start_ctid):
        end_ctid = self.must_include[tab][-1]

        if is_ctid_greater_than(start_ctid, end_ctid):
            self.must_include[tab].pop()
            return DONE  # tell caller to terminate

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
            self.insert_previous_ctid_list(self.must_include[tab], end_ctid, tab)
            return True
        else:
            _36 = self.must_include[tab].pop()
            _37 = self.may_exclude[tab].pop()
            if _37 not in self.must_include[tab]:
                self.must_include[tab].insert(0, _37)
            if _36 not in self.may_exclude[tab]:
                self.may_exclude[tab].insert(0, _36)
            self.insert_previous_ctid_list(self.must_include[tab], end_ctid, tab)  # _35
        return False

    def insert_previous_ctid_list(self, ctid_list, end_ctid, tab):
        prev_ctid = self.calculate_previous_ctid(end_ctid, tab)
        if prev_ctid is not None and not (prev_ctid in ctid_list):
            ctid_list.append(prev_ctid)

    def calculate_previous_ctid(self, end_ctid, tab):
        nctid = get_ctid_one_less(end_ctid)
        if nctid is None:
            nctid = self.connectionHelper.execute_sql_fetchone_0(
                select_previous_ctid(get_tabname_1(tab), end_ctid))
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
            # self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_1(tab))])
            self.view_drop_count = 0

    def calculate_mid_ctids(self, start_page, end_page, size, n=1):
        mid_page = int((start_page + end_page) * (n/self.ary))
        mid_ctid1 = "(" + str(mid_page) + ",1)"
        mid_ctid2 = "(" + str(mid_page) + ",2)"
        return mid_ctid1, mid_ctid2

    def create_view_execute_app_drop_view(self,
                                          end_ctid,
                                          mid_ctid_list,
                                          query,
                                          start_ctid,
                                          tabname,
                                          tabname1):
        ctid_list = make_total_ctid_list(end_ctid, mid_ctid_list, start_ctid)
        cut_count = len(mid_ctid_list)
        base_idx = [i for i in range(0, cut_count + 1)]
        combs = get_combinations(base_idx, cut_count)
        valid_ctid_range = None

        for idx in combs:
            param_ctids = []
            for i in idx:
                param_ctids.append([ctid_list[2 * i], ctid_list[2 * i + 1]])
            if self.check_result_for_ctid_range(param_ctids, tabname1, tabname, query):
                valid_ctid_range = param_ctids
                break
        # self.connectionHelper.execute_sql([drop_view(tabname)])
        return valid_ctid_range

    def format_insert_values(self, val):
        lval = []
        for v in val:
            self.logger.debug(v)
            if isinstance(v, Decimal):
                v1 = "{0:0.2f}".format(v)
                lval.append(v1)
                self.logger.debug(v1)
            elif isinstance(v, date):
                v1 = v.strftime("%Y-%m-%d")
                lval.append(v1)
            else:
                lval.append(v)
        return tuple(lval)
