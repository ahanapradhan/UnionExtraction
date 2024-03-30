from mysite.unmasque.refactored.util.abstract_queries import CommonQueries


class PostgresQueries(CommonQueries):
    DEBUG_QUERY = "select pid, state, query from pg_stat_activity where datname = 'tpch';"
    TERMINATE_STUCK_QUERIES = "SELECT pg_terminate_backend(pid);"

    def get_explain_query(self, sql):
        return f"EXPLAIN {sql}"

    def drop_table(self, tab):
        return f"drop table if exists {tab};"

    def drop_table_cascade(self, tab):
        return f"drop table if exists {tab} CASCADE;"

    def alter_table_rename_to(self, tab, retab):
        return f"Alter table {tab} rename to {retab};"

    def alter_view_rename_to(self, tab, retab):
        return f"Alter view {tab} rename to {retab};"

    def create_table_like(self, tab, ctab):
        return f"Create table {tab} (like {ctab});"

    def create_table_as_select_star_from(self, tab, fromtab):
        return f"Create table {tab} as select * from {fromtab};"

    def get_row_count(self, tab):
        return f"select count(*) from {tab};"

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
        return f"create table {tab} as select * from {fromtab} where ctid >= '{_start_citd}' and ctid <= '{_end_ctid}';"

    def get_ctid_from(self, min_or_max, tabname):
        return f"select {min_or_max}(ctid) from {tabname};"

    def truncate_table(self, table):
        return f"Truncate Table {table};"

    def insert_into_tab_attribs_format(self, att_order, esc_string, tab):
        return f"INSERT INTO {tab} {att_order}  VALUES {esc_string}"

    def update_tab_attrib_with_value(self, attrib, tab, value):
        str_value = str(value)
        return f"UPDATE {tab}  SET {attrib}={str_value};"

    def update_tab_attrib_with_quoted_value(self, tab, attrib, value):
        return f"UPDATE {tab}  SET {attrib} = '{value}';"

    def update_sql_query_tab_attribs(self, tab, attrib):
        tabname = str(tab)
        col = str(attrib)
        return f"update {tabname} set {col} = "

    def get_column_details_for_table(self, schema, tab):
        return f"select column_name, data_type, character_maximum_length from {schema}.information_schema.columns where table_name = '{tab}';"