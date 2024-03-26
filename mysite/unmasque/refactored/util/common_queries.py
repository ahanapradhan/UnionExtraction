from ..util.utils import get_format

GET_ALL_COLUMNS_QUERY = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND " \
                        "table_name   = 'part';"
DEBUG_QUERY = "select pid, state, query from pg_stat_activity where datname = 'tpch';"
TERMINATE_STUCK_QUERIES = "SELECT pg_terminate_backend(pid);"


def select_row_count_from_query(query):
    query = query.replace(";", "")
    return f"select count(*) from ({query}) as q;"


def insert_query_for_base_tables(stable, ftable, base_key, base_table, limit, perc):
    return f"insert into {stable} " \
           f"select * from {ftable} " \
           f"tablesample system({perc}) " \
           f"where ({base_key}) not in (select distinct({base_key}) from {base_table}) " \
           f"Limit {limit};"


def insert_query_for_not_sampled_tables(stable, ftable, key, base_key, base_table, limit):
    return f"insert into {stable} select * from {ftable} " \
           f"where {key} in (select distinct({base_key}) from {base_table}) " \
           f"and {key} not in (select distinct({key}) from {stable}) Limit {limit} ;"


def insert_into_sampletable_from_table_samplesize(stable, ftable, perc):
    return f"insert into {stable} " \
           f"select * from {ftable} tablesample system({perc});"


def drop_table(tab):
    return f"drop table if exists {tab};"


def drop_table_cascade(tab):
    return f"drop table if exists {tab} CASCADE;"


def alter_table_rename_to(tab, retab):
    return f"Alter table {tab} rename to {retab};"


def alter_view_rename_to(tab, retab):
    return f"Alter view {tab} rename to {retab};"


def create_table_like(tab, ctab):
    return f"Create table {tab} (like {ctab});"


def create_table_as_select_star_from(tab, fromtab):
    return f"Create table {tab} as select * from {fromtab};"


def create_table_as_select_star_from_limit_1(tab, fromtab):
    return f"Create table {tab} as select * from {fromtab} limit 1;"


def get_row_count(tab):
    return f"select count(*) from {tab};"


def get_star(tab):
    return f"select * from {tab};"


def get_star_from_except_all_get_star_from(tab1, tab2):
    return f"(select * from {tab1} except all select * from {tab2})"


def get_restore_name(tab):
    return f"{tab}_restore"


def get_min_max_ctid(tab):
    return f"select min(ctid), max(ctid) from {tab};"


def drop_view(tab):
    return f"drop view if exists {tab} cascade;"


def get_tabname_1(tab):
    return f"{tab}1"


def get_tabname_4(tab):
    return f"{tab}4"


def get_tabname_un(tab):
    return f"{tab}_un"


def get_tabname_nep(tab):
    return f"{tab}_nep"


def get_tabname_2(tab):
    return f"{tab}2"


def create_view_as(view, q):
    return f"create view {view}  as {q}"


def create_view_as_select_star_where_ctid(mid_ctid1, start_ctid, view, tab):
    _start_citd = str(start_ctid)
    _end_ctid = str(mid_ctid1)
    return f"create view {view} as select * from {tab} where ctid >= '{_start_citd}' and ctid <= '{_end_ctid}';"


def create_table_as_select_star_from_ctid(end_ctid, start_ctid, tab, fromtab):
    _start_citd = str(start_ctid)
    _end_ctid = str(end_ctid)
    return f"create table {tab} as select * from {fromtab} where ctid >= '{_start_citd}' and ctid <= '{_end_ctid}';"


def get_ctid_from(min_or_max, tabname):
    return f"select {min_or_max}(ctid) from {tabname};"


def truncate_table(table):
    return f"Truncate Table {table};"


def insert_into_tab_attribs_format(att_order, esc_string, tab):
    return f"INSERT INTO {tab} {att_order}  VALUES {esc_string}"
    # return f"INSERT INTO {tab} {att_order}  VALUES {esc_string}"


def update_tab_attrib_with_value(attrib, tab, value):
    str_value = str(value)
    return f"UPDATE {tab}  SET {attrib}={str_value};"


def update_tab_attrib_with_quoted_value(tab, attrib, value):
    return f"UPDATE {tab}  SET {attrib} = '{value}';"


def update_sql_query_tab_attribs(tab, attrib):
    tabname = str(tab)
    col = str(attrib)
    return f"update {tabname} set {col} = "


def form_update_query_with_value(update_string, datatype, val):
    update_val = get_format(datatype, val)
    return f"{update_string} {update_val};"


def get_column_details_for_table(schema, tab):
    return f"select column_name, data_type, character_maximum_length from " \
           f"information_schema.columns where table_schema = '{schema}' and " \
           f"table_name = '{tab}' order by column_name;"


def select_attribs_from_relation(tab_attribs, relation):
    attribs = ", ".join(tab_attribs)
    return f"select {attribs} from {relation};"


def insert_into_tab_select_star_fromtab(tab, fromtab):
    return f"Insert into {tab} Select * from {fromtab};"


def insert_into_tab_select_star_fromtab_with_ctid(tab, fromtab, ctid):
    return f"Insert into {tab} (Select * from {fromtab} Where ctid = {ctid});"


def select_ctid_from_tabname_offset(tabname, offset):
    return f"Select ctid from {tabname} offset {offset} Limit 1;"


def select_next_ctid(tabname, mid_ctid1):
    return f"Select Min(ctid) from {tabname} Where ctid > '{mid_ctid1}';"


def select_previous_ctid(tab, ctid1):
    return f"Select MAX(ctid) from {tab} Where ctid < '{ctid1}';"


def select_max_ctid(tab):
    return f"Select MAX(ctid) from {tab};"


def select_start_ctid_of_any_table():
    return '(0,1)'


def hashtext_query(tab):
    return f"select sum(hashtext) from (select hashtext({tab}::TEXT) FROM {tab}) as T;"
