from ...refactored.util.abstract_queries import CommonQueries


class OracleQueries(CommonQueries):

    def get_explain_query(self, sql):
        sql = sql.replace(";", "")
        return f"EXPLAIN PLAN {sql}"

    def drop_table(self, tab):
        return f"DROP TABLE {tab} CASCADE CONSTRAINTS"

    def drop_table_cascade(self, tab):
        return self.drop_table(tab)  # In Oracle, DROP TABLE already includes cascade.

    def alter_table_rename_to(self, tab, retab):
        return f"ALTER TABLE {tab} RENAME TO {retab}"

    def alter_view_rename_to(self, tab, retab):
        return f"ALTER VIEW {tab} RENAME TO {retab}"

    def create_table_like(self, tab, ctab):
        return f"CREATE TABLE {tab} AS SELECT * FROM {ctab} WHERE 1=0"

    def create_table_as_select_star_from(self, tab, fromtab):
        return f"CREATE TABLE {tab} AS SELECT * FROM {fromtab}"

    def get_row_count(self, tab):
        return f"SELECT COUNT(*) FROM {tab}"

    def get_star(self, tab):
        return f"SELECT * FROM {tab}"

    def get_star_from_except_all_get_star_from(self, tab1, tab2):
        return f"SELECT * FROM {tab1} MINUS SELECT * FROM {tab2}"

    def get_min_max_ctid(self, tab):
        # Oracle uses ROWID instead of CTID. Adjust according to your requirements.
        return f"SELECT MIN(ROWID), MAX(ROWID) FROM {tab}"

    def drop_view(self, tab):
        return f"DROP VIEW {tab}"

    def create_view_as(self, view, q):
        return f"CREATE VIEW {view} AS {q}"

    def create_view_as_select_star_where_ctid(self, mid_ctid1, start_ctid, view, tab):
        # Adjusted for Oracle, using ROWID as an example.
        return f"CREATE VIEW {view} AS SELECT * FROM {tab} WHERE ROWID BETWEEN '{start_ctid}' AND '{mid_ctid1}'"

    def create_table_as_select_star_from_ctid(self, end_ctid, start_ctid, tab, fromtab):
        # Adjusted for Oracle, using ROWID as an example.
        return f"CREATE TABLE {tab} AS SELECT * FROM {fromtab} WHERE ROWID BETWEEN '{start_ctid}' AND '{end_ctid}'"

    def get_ctid_from(self, min_or_max, tabname):
        # Adjusted for Oracle, using ROWID as an example.
        return f"SELECT {min_or_max}(ROWID) FROM {tabname}"

    def truncate_table(self, table):
        return f"TRUNCATE TABLE {table}"

    def insert_into_tab_attribs_format(self, att_order, esc_string, tab):
        return f"INSERT INTO {tab} ({att_order}) VALUES ({esc_string})"

    def update_tab_attrib_with_value(self, attrib, tab, value):
        return f"UPDATE {tab} SET {attrib} = {value}"

    def update_tab_attrib_with_quoted_value(self, tab, attrib, value):
        return f"UPDATE {tab} SET {attrib} = '{value}'"

    def update_sql_query_tab_attribs(self, tab, attrib):
        return f"UPDATE {tab} SET {attrib} = "

    def get_column_details_for_table(self, schema, tab):
        return f"SELECT column_name, data_type, data_length FROM all_tab_columns WHERE table_name = UPPER('{tab}') AND owner = UPPER('{schema}')"