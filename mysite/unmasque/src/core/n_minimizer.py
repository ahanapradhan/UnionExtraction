from _decimal import Decimal

from ..util.constants import DONE, NO_REDUCTION
from ...refactored.abstract.MinimizerBase import Minimizer
from ...refactored.util.common_queries import alter_table_rename_to, get_tabname_1, drop_view, select_previous_ctid, \
    alter_view_rename_to, create_table_as_select_star_from, \
    get_tabname_2, get_star, drop_table, drop_table_cascade


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
            self.minimize_table(query, tab)
            res, des = self.connectionHelper.execute_sql_fetchall(get_star(tab))
            for row in res:
                self.logger.debug(row)
        return True

    def minimize_table(self, query, tab):
        end_ctid, start_ctid = self.do_binary_halving_till_possible(query, tab)
        if end_ctid is None:
            return

        size = self.core_sizes[tab]
        self.logger.debug(start_ctid, end_ctid)

        self.may_exclude[tab] = []  # set of tuples that can be removed for getting non empty result
        self.must_include[tab] = []  # set of tuples that must be preserved for getting non empty result

        # first time
        self.may_exclude[tab].append(end_ctid)
        c2tid = self.connectionHelper.execute_sql_fetchone_0(select_previous_ctid(tab, end_ctid))
        self.must_include[tab].append(c2tid)

        self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_1(tab))])
        check = self.do_row_by_row_elimination(query, start_ctid, tab)
        if check == DONE or check == NO_REDUCTION:
            self.connectionHelper.execute_sql([drop_view(tab),
                                               alter_table_rename_to(get_tabname_1(tab), tab)])
            return
        else:
            must_ctid = check
            row, _ = self.connectionHelper.execute_sql_fetchall(f"select * from {get_tabname_1(tab)} where ctid = '{must_ctid}';")
            # convert the view into a table
            self.connectionHelper.execute_sql([alter_view_rename_to(tab, get_tabname_2(tab)),
                                               create_table_as_select_star_from(tab, get_tabname_2(tab))])
            self.logger.debug(tab, " is now a table")
            for val in row:
                self.logger.debug(val)
                fval = self.format_decimals(val)
                self.connectionHelper.execute_sql([f"Insert into {tab} values {fval};"])
            if self.core_sizes[tab] < size:
                self.connectionHelper.execute_sql([drop_table_cascade(get_tabname_1(tab))])
                self.minimize_table(query, tab)

        self.core_sizes[tab] = len(self.must_include[tab])
        self.logger.debug(self.core_sizes[tab])

    def do_row_by_row_elimination(self, query, start_ctid, tab):
        mandatory_tuple = self.must_include[tab][-1]
        size = self.core_sizes[tab]
        while True:
            self.connectionHelper.execute_sql([drop_view(tab)])
            self.swicth_transaction(tab)
            ok = self.is_ok_to_eliminate_previous_tuple(tab, query, start_ctid)
            if ok == DONE:
                return DONE
            self.logger.debug("may exclude", self.may_exclude[tab])
            self.logger.debug("must include", self.must_include[tab])
            if ok and size == self.core_sizes[tab]:
                return NO_REDUCTION
            if not ok and self.must_include[tab][0] != mandatory_tuple:
                '''
                found another tuple which needs to be preserved. 
                Now we can try bin halving on the remaining data to speed up the rest of the search
                '''
                return self.must_include[tab][0]

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
            self.must_include[tab].append(self.calculate_previous_ctid(end_ctid, tab))
            return True
        else:
            self.must_include[tab].insert(0, self.may_exclude[tab].pop())
            self.may_exclude[tab].append(self.calculate_previous_ctid(end_ctid, tab))
        return False

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

    def format_decimals(self, val):
        lval = []
        for v in val:
            self.logger.debug(v)
            if isinstance(v, Decimal):
                v1 = "{0:0.2f}".format(v)
                lval.append(v1)
                self.logger.debug(v1)
            else:
                lval.append(v)
        return tuple(lval)

                
