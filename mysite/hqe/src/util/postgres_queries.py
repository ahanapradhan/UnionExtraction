from .abstract_queries import CommonQueries
from .utils import get_format


class PostgresQueries(CommonQueries):
    def analyze_table(self, tab):
        return f"ANALYZE {tab};"

    def create_table_as_select_star_from_where(self, tab, fromtab, where):
        return f"Create table {tab} as (Select * from {fromtab} where {where}); "  # \
        # f"ALTER TABLE {tab} SET (autovacuum_enabled = false);"

    def create_table_as_select_star_from_limit_1(self, tab, fromtab):
        return f"Create table {tab} as (Select * from {fromtab} limit 1); "  # \
        # f"ALTER TABLE {tab} SET (autovacuum_enabled = false);"

    def form_update_query_with_value(self, update_string, datatype, val):
        update_val = get_format(datatype, val)
        return f"{update_string} {update_val};"

    DEBUG_QUERY = "select pid, state, query from pg_stat_activity where datname = 'tpch';"
    TERMINATE_STUCK_QUERIES = "SELECT pg_terminate_backend(pid);"

    def get_explain_query(self, sql):
        return f"EXPLAIN {sql}"

    def drop_table(self, tab):
        return f"drop table if exists {tab};"

    def drop_table_cascade(self, tab):
        return f"drop table if exists {tab} CASCADE;"

    def alter_table_rename_to(self, tab, retab):
        return f"Alter table if exists {tab} rename to {retab};"

    def alter_view_rename_to(self, tab, retab):
        return f"Alter view {tab} rename to {retab};"

    def create_table_like(self, tab, ctab):
        return f"Create table if not exists {tab} (like {ctab}); "  # \
        # f"ALTER TABLE {tab} SET (autovacuum_enabled = false);"

    def create_table_as_select_star_from(self, tab, fromtab):
        return f"Create table if not exists {tab} as select * from {fromtab}; "  # \
        # f"ALTER TABLE {tab} SET (autovacuum_enabled = false);"

    def get_row_count(self, tab):
        return f"select count(*) from {tab};"

    def get_non_null_row_count(self, tab):
        pass

    def get_star(self, tab):
        return f"select * from {tab};"

    def get_star_from_except_all_get_star_from(self, tab1, tab2):
        return f"(select * from {tab1} except all select * from {tab2})"

    def get_min_max_ctid(self, tab):
        return f"select min(ctid), max(ctid) from {tab};"

    def drop_view(self, tab):
        return f"drop view if exists {tab} cascade;"

    def create_view_as(self, view, q):
        return f"create view {view}  as {q}"

    def create_view_as_select_star_where_ctid(self, mid_ctid1, start_ctid, view, tab):
        _start_citd = str(start_ctid)
        _end_ctid = str(mid_ctid1)
        return f"create view {view} as select * from {tab} where ctid >= '{_start_citd}' and ctid <= '{_end_ctid}';"

    def create_table_as_select_star_from_ctid(self, end_ctid, start_ctid, tab, fromtab):
        _start_citd = str(start_ctid)
        _end_ctid = str(end_ctid)
        return f"create table {tab} as select * from {fromtab} " \
               f"where ctid >= '{_start_citd}' and ctid <= '{_end_ctid}'; "  # \
        # f"ALTER TABLE {tab} SET (autovacuum_enabled = false);"

    def get_ctid_from(self, min_or_max, tabname):
        return f"select {min_or_max}(ctid) from {tabname};"

    def truncate_table(self, table):
        return f"Truncate Table {table};"

    def insert_into_tab_attribs_format(self, att_order, esc_string, tab):
        if esc_string == "":
            _count = att_order.count(",")
            esc_list = [f"%s"] * (_count + 1)
            esc_string = ",".join(esc_list)
            esc_string = f"({esc_string})"
        return f"INSERT INTO {tab} {att_order}  VALUES {esc_string};"

    def update_key_attrib_with_val(self, tab, attrib, value, prev, qoted):
        if qoted:
            value = f"'{value}'"
            prev = f"'{prev}'"
        query = f"UPDATE {tab} SET {attrib} = CASE WHEN {attrib} = {prev} THEN {value} WHEN {attrib} = {value} THEN {prev} ELSE {attrib} END;"
        return query

    def update_tab_attrib_with_value(self, tab, attrib, value):
        str_value = str(value)
        query = f"UPDATE {tab}  SET {attrib}={str_value};"
        # print(query)
        return query

    def update_tab_attrib_with_quoted_value(self, tab, attrib, value):
        query = f"UPDATE {tab}  SET {attrib}= '{value}';"
        # print(query)
        return query

    def update_sql_query_tab_attribs(self, tab, attrib):
        tabname = str(tab)
        col = str(attrib)
        return f"update {tabname} set {col} = "

    def get_column_details_for_table(self, schema, tab):
        return f"select column_name, data_type, character_maximum_length from information_schema.columns where " \
               f"table_schema = '{schema}' and table_name = '{tab}';"

    def select_attribs_from_relation(self, tab_attribs, relation):
        attribs = ", ".join(tab_attribs)
        return f"select {attribs} from {relation};"

    def insert_into_tab_select_star_fromtab(self, tab, fromtab):
        return f"Insert into {tab} Select * from {fromtab};"

    def insert_into_tab_select_star_fromtab_with_ctid(self, tab, fromtab, ctid):
        return f"Insert into {tab} (Select * from {fromtab} Where ctid = {ctid});"

    def select_ctid_from_tabname_offset(self, tabname, offset):
        return f"Select ctid from {tabname} offset {offset} Limit 1;"

    def select_next_ctid(self, tabname, mid_ctid1):
        return f"Select Min(ctid) from {tabname} Where ctid > '{mid_ctid1}';"

    def select_previous_ctid(self, tab, ctid1):
        return f"Select MAX(ctid) from {tab} Where ctid < '{ctid1}';"

    def select_max_ctid(self, tab):
        return f"Select MAX(ctid) from {tab};"

    def select_start_ctid_of_any_table(self):
        return '(0,1)'

    def hashtext_query(self, tab):
        return f"select sum(hashtext) from (select hashtext({tab}::TEXT) FROM {tab}) as T;"
