def select_row_count_from_query(query):
    query = query.replace(";", "")
    return f"select count(*) from ({query}) q"


def insert_query_for_base_tables(stable, ftable, base_key, base_table, limit, perc):
    return f"insert into {stable} " \
           f"select * from {ftable} " \
           f"sample({perc}) " \
           f"where ({base_key}) not in (select distinct({base_key}) from {base_table}) " \
           f"and rownum <= {limit}"


def insert_query_for_not_sampled_tables(stable, ftable, key, base_key, base_table, limit):
    return f"insert into {stable} select * from {ftable} " \
           f"where {key} in (select distinct({base_key}) from {base_table}) " \
           f"and {key} not in (select distinct({key}) from {stable}) and rownum <= {limit}"


def insert_into_sampletable_from_table_samplesize(stable, ftable, perc):
    return f"insert into {stable} " \
           f"select * from {ftable} sample({perc})"


def drop_table(tab):
    return f"drop table {tab}"


def drop_table_cascade(tab):
    return f"drop table {tab} cascade constraints"


def alter_table_rename_to(tab, retab):
    return f"alter table {tab} rename to {retab}"


def alter_view_rename_to(tab, retab):
    return f"alter view {tab} rename to {retab}"


def create_table_like(tab, ctab):
    return f"create table {tab} as select * from {ctab} where 1=0"


def create_table_as_select_star_from(tab, fromtab):
    return f"create table {tab} as select * from {fromtab} where 1=0"


def create_table_as_select_star_from_limit_1(tab, fromtab):
    return f"create table {tab} as select * from {fromtab} where rownum <= 1"


def get_row_count(tab):
    return f"select count(*) from {tab}"


def get_star(tab):
    return f"select * from {tab}"


def get_star_from_except_all_get_star_from(tab1, tab2):
    return f"(select * from {tab1} minus all select * from {tab2})"


def get_restore_name(tab):
    return f"{tab}_restore"


def get_min_max_ctid(tab):
    return f"select min(rowid), max(rowid) from {tab}"


def drop_view(tab):
    return f"drop view {tab}"


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
    return f"create view {view} as {q}"


def create_view_as_select_star_where_ctid(mid_ctid1, start_ctid, view, tab):
    return f"create view {view} as select * from {tab} where rowid between '{start_ctid}' and '{mid_ctid1}'"


def create_table_as_select_star_from_ctid(end_ctid, start_ctid, tab, fromtab):
    return f"create table {tab} as select * from {fromtab} where rowid between '{start_ctid}' and '{end_ctid}'"


def get_ctid_from(min_or_max, tabname):
    if min_or_max == "min":
        return f"select min(rowid) from {tabname}"
    elif min_or_max == "max":
        return f"select max(rowid) from {tabname}"


def truncate_table(table):
    return f"truncate table {table}"


def insert_into_tab_attribs_format(att_order, esc_string, tab):
    return f"insert into {tab} ({att_order}) values ({esc_string})"


def update_tab_attrib_with_value(attrib, tab, value):
    return f"update {tab} set {attrib} = {value}"


def update_tab_attrib_with_quoted_value(tab, attrib, value):
    return f"update {tab} set {attrib} = '{value}'"


def update_sql_query_tab_attribs(tab, attrib):
    return f"update {tab} set {attrib} = "


def form_update_query_with_value(update_string, datatype, val):
    return f"{update_string} {val}"


def get_column_details_for_table(schema, tab):
    return f"select column_name, data_type, data_length from " \
           f"user_tab_columns where table_name = '{tab}'"


def select_attribs_from_relation(tab_attribs, relation):
    attribs = ", ".join(tab_attribs)
    return f"select {attribs} from {relation}"


def insert_into_tab_select_star_fromtab(tab, fromtab):
    return f"insert into {tab} select * from {fromtab}"


def insert_into_tab_select_star_fromtab_with_ctid(tab, fromtab, ctid):
    return f"insert into {tab} select * from {fromtab} where rowid = '{ctid}'"


def select_ctid_from_tabname_offset(tabname, offset):
    return f"select rowid from {tabname} where rownum <= {offset + 1}"


def select_next_ctid(tabname, mid_ctid1):
    return f"select min(rowid) from {tabname} where rowid > '{mid_ctid1}'"


def select_previous_ctid(tab, ctid1):
    return f"select max(rowid) from {tab} where rowid < '{ctid1}'"


def select_max_ctid(tab):
    return f"select max(rowid) from {tab}"


def select_start_ctid_of_any_table():
    return "'0000000000AAAAAAAB'"


def hashtext_query(tab):
    return f"select sum(hashtext(rowid)) from {tab}"
