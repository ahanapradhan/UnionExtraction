import copy
import os

import pandas as pd

from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.common_queries import get_row_count, alter_table_rename_to, get_min_max_ctid, \
    drop_view, drop_table, create_table_as_select_star_from, get_ctid_from, get_tabname_1, \
    create_view_as_select_star_where_ctid, create_table_as_select_star_from_ctid, get_tabname_4, get_star, \
    get_restore_name
from mysite.unmasque.refactored.util.utils import isQ_result_empty


def extract_start_and_end_page(rctid):
    min_ctid = rctid[0]
    min_ctid2 = min_ctid.split(",")
    start_page = int(min_ctid2[0][1:])
    max_ctid = rctid[1]
    print(max_ctid)
    max_ctid2 = max_ctid.split(",")
    end_page = int(max_ctid2[0][1:])
    start_ctid = min_ctid
    end_ctid = max_ctid
    return end_ctid, end_page, start_ctid, start_page


class ViewMinimizer(Base):
    max_row_no = 1
    global_reduced_data_path = "/Users/ahanapradhan/Desktop/Projects/UNMASQUE/reduced_data/"

    def __init__(self, connectionHelper,
                 core_relations,
                 sampling_status):
        super().__init__(connectionHelper, "View_Minimizer")
        self.global_other_info_dict = {}
        self.global_result_dict = {}
        self.local_other_info_dict = {}
        self.global_min_instance_dict = {}
        self.app = Executable(connectionHelper)
        self.core_relations = core_relations
        self.cs2_passed = sampling_status

    def getCoreSizes(self):
        core_sizes = {}
        for table in self.core_relations:
            try:
                core_sizes[table] = self.connectionHelper.execute_sql([get_row_count(table)])
            except Exception as error:
                print("Error in getting table Sizes. Error: " + str(error))
        return core_sizes

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
        print("REMAINING TABLE SIZE", core_sizes[tabname])
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
            res = self.connectionHelper.execute_sql_fetchall(get_star(tabname))
            self.connectionHelper.execute_sql([create_table_as_select_star_from(get_tabname_4(tabname), tabname)])
            print(tabname, "==", res)

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


"""
def reduce_Database_Instance_cs_fail(core_relations, method='binary partition', max_no_of_rows=1, executable_path=""):
    reveal_globals.local_other_info_dict = {}
    # Perform sampling
    # print(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter,"++++SAMPLING++++++++")
    # core_sizes = sample_Database_Instance(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter)
    # print("sneha here")
    # exit(0)

    # core_sizes = getCoreSizes(core_relations)
    core_sizes = reveal_globals.global_core_sizes
    start_time = time.time()
    # print("YES1")

    print("xhcbhcb")
    '''
    cur = reveal_globals.global_conn.cursor()
    cur.execute("set synchronize_seqscans = 'OFF';")
    cur.close()
    '''
    for tabname in reveal_globals.global_core_relations:

        cur = reveal_globals.global_conn.cursor()
        cur.execute('alter table ' + tabname + ' rename to ' + tabname + '_restore ;')
        # cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        cur.execute('select min(ctid), max(ctid) from ' + tabname + '_restore; ')
        rctid = cur.fetchone()
        cur.close()
        min_ctid = rctid[0]
        # print(min_ctid)
        min_ctid2 = min_ctid.split(",")
        start_page = int(min_ctid2[0][1:])
        max_ctid = rctid[1]
        print(max_ctid)
        max_ctid2 = max_ctid.split(",")
        end_page = int(max_ctid2[0][1:])

        # cur = reveal_globals.global_conn.cursor()
        # cur.execute('select max(ctid) from '+ tabname+'1; ')
        # max_ctid =cur.fetchone()
        # cur.close()
        # max_ctid = max_ctid[0]
        # print(max_ctid)
        # max_ctid2 = max_ctid.split(",")
        # end_page = int(max_ctid2[0][1:])

        start_ctid = min_ctid
        end_ctid = max_ctid
        print("start_page= ", start_page, "end page= ", end_page)
        while (start_page < end_page - 1):
            mid_page = int((start_page + end_page) / 2)
            mid_ctid1 = "(" + str(mid_page) + ",1)"
            mid_ctid2 = "(" + str(mid_page) + ",2)"

            cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            cur.execute("create view " + tabname + " as select * from " + tabname + "_restore where ctid >= '" + str(
                start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
            cur.close()

            # Run query and analyze the result now

            # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            # cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            # cur.close()

            # new_result = executable.getExecOutput()
            new_result_flag = getExecOutput()

            # if len(new_result) <= 1:
            if new_result_flag == False:
                # Take the lower half
                start_ctid = mid_ctid2
            else:
                # Take the upper half
                end_ctid = mid_ctid1
            # start_page=start_ctid[0]

            # UN+nf
            # if check_nullfree.getExecOutput() == False:
            # 	#Take the lower half
            # 	start_ctid = mid_ctid2
            # else:
            # 	#Take the upper half
            # 	end_ctid = mid_ctid1

            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop view ' + tabname + ';')
            cur.close()

            start_ctid2 = start_ctid.split(",")
            start_page = int(start_ctid2[0][1:])
            end_ctid2 = end_ctid.split(",")
            end_page = int(end_ctid2[0][1:])
            print("start_page= ", start_page, "end page= ", end_page)
            print(start_ctid, end_ctid)
        cur = reveal_globals.global_conn.cursor()

        # cur.execute('drop view '+ tabname + ';')
        print("create table " + tabname + " as select * from " + tabname + "_restore where ctid >= '" + str(
            start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
        cur.execute("create table " + tabname + " as select * from " + tabname + "_restore where ctid >= '" + str(
            start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
        # cur.execute('drop table ' + tabname + '1 ;')
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        cur.execute("select count(*) from " + tabname + ";")
        size = int(cur.fetchone()[0])
        cur.close()
        core_sizes[tabname] = size
        print("REMAINING TABLE SIZE", core_sizes[tabname])

        print(start_ctid, end_ctid)

        while int(core_sizes[tabname]) > max_no_of_rows:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('alter table ' + tabname + ' rename to ' + tabname + '1 ;')
            # cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
            cur.close()

            cur = reveal_globals.global_conn.cursor()
            cur.execute('select min(ctid) from ' + tabname + '1; ')
            min_ctid = cur.fetchone()
            cur.close()
            min_ctid = min_ctid[0]
            min_ctid = min_ctid[1:-1]
            min_ctid2 = min_ctid.split(",")
            print(min_ctid2)
            start_row = int(min_ctid2[1])
            start_page = int(min_ctid2[0])

            cur = reveal_globals.global_conn.cursor()
            cur.execute('select max(ctid) from ' + tabname + '1; ')
            max_ctid = cur.fetchone()
            cur.close()
            max_ctid = max_ctid[0]
            max_ctid = max_ctid[1:-1]
            max_ctid2 = max_ctid.split(",")
            print(max_ctid2)
            end_row = int(max_ctid2[1])
            end_page = int(max_ctid2[0])
            print("start_row= ", start_row, "end_row = ", end_row, "#########")

            # mid_page=int((start_page + end_page)/2)
            start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
            end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"

            # if start_page!= end_page:
            # 	#wish to know last row in start page
            # 	mid_ctid1="(" + str(end_page) + "," + str(0) + ")"
            # 	mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"
            mid_row = int(core_sizes[tabname] / 2)

            mid_ctid1 = "(" + str(0) + "," + str(mid_row) + ")"
            mid_ctid2 = "(" + str(0) + "," + str(mid_row + 1) + ")"

            cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            cur.execute("create view " + tabname + " as select * from " + tabname + "1 where ctid >= '" + str(
                start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
            cur.close()

            # Run query and analyze the result now

            # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            # cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            # cur.close()

            # new_result = executable.getExecOutput()
            new_result_flag = getExecOutput()

            # if len(new_result) <= 1:
            if new_result_flag == False:
                # Take the lower half
                start_ctid = mid_ctid2
            else:
                # Take the upper half
                end_ctid = mid_ctid1
            # start_page=start_ctid[0]

            # UN+nf
            # if check_nullfree.getExecOutput() == False:
            # 	#Take the lower half
            # 	start_ctid = mid_ctid2
            # else:
            # 	#Take the upper half
            # 	end_ctid = mid_ctid1

            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop view ' + tabname + ';')
            cur.close()

            cur = reveal_globals.global_conn.cursor()
            cur.execute("create table " + tabname + " as select * from " + tabname + "1 where ctid >= '" + str(
                start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
            cur.execute('drop table ' + tabname + '1 ;')
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            cur.execute("select count(*) from " + tabname + ";")
            size = int(cur.fetchone()[0])
            cur.close()
            core_sizes[tabname] = size
            print("REMAINING TABLE SIZE", core_sizes[tabname])

        # SANITY CHECK
        # new_result = executable.getExecOutput()
        new_result_flag = getExecOutput()

        # if len(new_result) <= 1:
        if new_result_flag == False:
            print("Error: Query out of extractable domain\n")
            return False

    # WRITE TO Reduced Data Directory
    # check for data directory existence, if not exists , create it
    if not os.path.exists(reveal_globals.global_reduced_data_path):
        os.makedirs(reveal_globals.global_reduced_data_path)
    for tabname in core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("select * from " + tabname + ";")
        res = cur.fetchall()
        # cur.execute(" COPY " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        # cur.execute('drop table'+ tabname + "4 )
        cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
        cur.close()
        print(tabname, "==", res)
    # SANITY CHECK
    new_result = executable.getExecOutput()
    if len(new_result) <= 1:
        print("Error: Query out of extractable domain\n")
        return False
    # if check_nullfree.getExecOutput() == False:
    # 	print("Error: Query out of extractable domain\n")
    # 	return False

    # populate screen data
    # POPULATE MIN INSTANCE DICT
    reveal_globals.view_min_time = time.time() - start_time
    conn = reveal_globals.global_conn
    for tabname in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[tabname] = []
        sql_query = pd.read_sql_query("select * from " + tabname + ";", conn)
        df = pd.DataFrame(sql_query)
        reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
        for index, row in df.iterrows():
            reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
    # populate other data
    new_result = executable.getExecOutput()
    reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
    reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
    reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
    return True
"""
