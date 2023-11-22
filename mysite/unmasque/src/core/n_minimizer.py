from _decimal import Decimal
from datetime import date

from ..util.constants import DONE
from ...refactored.abstract.MinimizerBase import Minimizer
from ...refactored.util.common_queries import alter_table_rename_to, get_tabname_1, drop_view, select_previous_ctid, \
    get_row_count, select_start_ctid_of_any_table, drop_table


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

    def do_minimizeJob(self, query):
        for tab in self.core_relations:
            self.minimize_table(query, tab)
        return True

    def minimize_table(self, query, tab):
        end_ctid, start_ctid = self.do_binary_halving_till_possible(query, tab)
        if end_ctid is None:
            return

        self.logger.debug(start_ctid, end_ctid)

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
            size = self.core_sizes[tab]
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

    def do_binary_halving_till_possible(self, query, tab):
        while True:
            tab_size = self.core_sizes[tab]
            end_ctid, start_ctid = self.try_binary_halving(query, tab)
            if end_ctid is None:  # could not reduce anymore
                break
            self.core_sizes = self.update_with_remaining_size(self.core_sizes, end_ctid,
                                                              start_ctid, tab, get_tabname_1(tab))
            self.logger.debug(tab_size, self.core_sizes[tab])
            if tab_size == self.core_sizes[tab]:  # did not reduce anymore
                break
        return end_ctid, start_ctid

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
        # self.connectionHelper.execute_sql([drop_view(tabname)])
        return end_ctid, start_ctid

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
