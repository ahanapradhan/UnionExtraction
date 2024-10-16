from .abstract_queries import CommonQueries
from .utils import get_format


class OracleQueries(CommonQueries):
    def get_non_null_row_count(self, tab):
        pass

    def update_key_attrib_with_val(self, tab, attrib, value, prev, qoted):
        pass

    def analyze_table(self, tab):
        pass

    schema = None

    def create_table_as_select_star_from_where(self, tab, fromtab, where):
        return f"Create table {self.schema}.{tab} as (Select * from {self.schema}.{fromtab} where {where})"

    def set_schema(self, schema):
        self.schema = schema

    def create_table_as_select_star_from_limit_1(self, tab, fromtab):
        return f"Create table {self.schema}.{tab} as (select * from {self.schema}.{fromtab} where ROWNUM = 1)"

    def get_explain_query(self, sql):
        sql = sql.replace(";", "")
        return f"EXPLAIN PLAN {sql}"

    def drop_table(self, tab):
        return f"DROP TABLE {self.schema}.{tab}"

    def drop_table_cascade(self, tab):
        return self.drop_table(tab)  # In Oracle, DROP TABLE already includes cascade.

    def alter_table_rename_to(self, tab, retab):
        return f"ALTER TABLE {self.schema}.{tab} RENAME TO {retab}"

    def alter_view_rename_to(self, tab, retab):
        return f"ALTER VIEW {self.schema}.{tab} RENAME TO {retab}"

    def create_table_like(self, tab, ctab):
        return f"CREATE TABLE {self.schema}.{tab} AS SELECT * FROM {self.schema}.{ctab} WHERE 1=0"

    def create_table_as_select_star_from(self, tab, fromtab):
        return f"CREATE TABLE {self.schema}.{tab} AS SELECT * FROM {self.schema}.{fromtab}"

    def get_row_count(self, tab):
        return f"SELECT COUNT(*) FROM {self.schema}.{tab}"

    def get_star(self, tab):
        return f"SELECT * FROM {self.schema}.{tab}"

    def get_star_from_except_all_get_star_from(self, tab1, tab2):
        return f"SELECT * FROM {self.schema}.{tab1} MINUS SELECT * FROM {self.schema}.{tab2}"

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
        return f"TRUNCATE TABLE {self.schema}.{table}"

    def insert_into_tab_attribs_format(self, att_order, esc_string, tab):
        return f"INSERT INTO {self.schema}.{tab} {att_order} VALUES "

    def update_tab_attrib_with_value(self, tab, attrib, value):
        update_query = f"UPDATE {self.schema}.{tab} SET {attrib} = {value}"
        return update_query

    def form_update_query_with_value(self, update_string, datatype, val):
        update_val = get_format(datatype, val)
        query = f"{update_string} {update_val}"
        return query

    def update_tab_attrib_with_quoted_value(self, tab, attrib, value):
        if not len(value):
            value = " "
        return f"UPDATE {self.schema}.{tab} SET {attrib} = '{value}'"

    def update_sql_query_tab_attribs(self, tab, attrib):
        return f"UPDATE {self.schema}.{tab} SET {attrib} = "

    def update_sql_query_tab_date_attrib_value(self, tab, attrib, value):
        return f"UPDATE {self.schema}.{tab} SET {attrib} = TO_DATE({value}, 'YYYY-MM-DD')"

    def get_column_details_for_table(self, schema, tab):
        return f"SELECT column_name, data_type, data_length FROM all_tab_columns WHERE table_name = '{tab.upper()}' AND owner = '{schema.upper()}'"

    def select_attribs_from_relation(self, tab_attribs, relation):
        attribs = ", ".join(tab_attribs)
        return f"SELECT {attribs} FROM {self.schema}.{relation}"

    def insert_into_tab_select_star_fromtab(self, tab, fromtab):
        return f"INSERT INTO {tab} SELECT * FROM {fromtab}"

    def insert_into_tab_select_star_fromtab_with_ctid(self, tab, fromtab, ctid):
        # Oracle uses ROWID instead of ctid. Adjust the query accordingly.
        return f"INSERT INTO {tab} SELECT * FROM {fromtab} WHERE ROWID = '{ctid}'"

    def select_ctid_from_tabname_offset(self, tabname, offset):
        # Oracle does not support OFFSET in the same way. Use ROWNUM and a subquery for equivalent functionality.
        return f"SELECT ROWID FROM (SELECT ROWID, ROWNUM AS rn FROM {tabname}) WHERE rn = {int(offset) + 1}"

    def select_next_ctid(self, tabname, mid_ctid1):
        # Adjusted for Oracle using ROWID.
        return f"SELECT MIN(ROWID) FROM {tabname} WHERE ROWID > '{mid_ctid1}'"

    def select_previous_ctid(self, tab, ctid1):
        # Adjusted for Oracle using ROWID.
        return f"SELECT MAX(ROWID) FROM {tab} WHERE ROWID < '{ctid1}'"

    def select_max_ctid(self, tab):
        # Adjusted for Oracle using ROWID.
        return f"SELECT MAX(ROWID) FROM {tab}"

    def select_start_ctid_of_any_table(self):
        # Oracle's ROWID is not numeric, so this concept does not directly translate.
        # You might need a different approach based on your specific use case.
        return 'ROWID of the first row'

    def hashtext_query(self, tab):
        # Oracle does not have a direct equivalent of PostgreSQL's hashtext function. You might need to use
        # DBMS_CRYPTO.HASH or a custom implementation. Example using DBMS_CRYPTO (assuming you want to hash the
        # entire row concatenated as text, which needs careful consideration):
        return f"SELECT SUM(ora_hash(row_data)) FROM (SELECT DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(TO_CHAR({tab})), 3) AS row_data FROM {tab})"
