from abc import ABC, abstractmethod


class CommonQueries(ABC):

    def select_row_count_from_query(self, query):
        return f"select count(*) from ({query});"

    @abstractmethod
    def get_explain_query(self, sql):
        pass

    @abstractmethod
    def drop_table(self, tab):
        pass

    @abstractmethod
    def drop_table_cascade(self, tab):
        pass

    @abstractmethod
    def alter_table_rename_to(self, tab, retab):
        pass

    @abstractmethod
    def alter_view_rename_to(self, tab, retab):
        pass

    @abstractmethod
    def create_table_like(self, tab, ctab):
        pass

    @abstractmethod
    def create_table_as_select_star_from(self, tab, fromtab):
        pass

    @abstractmethod
    def get_row_count(self, tab):
        pass

    @abstractmethod
    def get_star(self, tab):
        pass

    @abstractmethod
    def get_star_from_except_all_get_star_from(self, tab1, tab2):
        pass

    def get_restore_name(self, tab):
        return f"{tab}_restore"

    def get_backup(self, tab):
        return f"{tab}_backup"

    @abstractmethod
    def get_min_max_ctid(self, tab):
        pass

    @abstractmethod
    def drop_view(self, tab):
        pass

    def get_tabname_nep_min(self, tab):
        return f"{tab}_nep_min"

    def get_tabname_1(self, tab):
        return f"{tab}1"

    def get_tabname_4(self, tab):
        return f"{tab}4"

    def get_tabname_un(self, tab):
        return f"{tab}_un"

    def get_tabname_nep(self, tab):
        return f"{tab}_nep"

    def get_tabname_2(self, tab):
        return f"{tab}2"

    @abstractmethod
    def create_view_as(self, view, q):
        pass

    @abstractmethod
    def create_view_as_select_star_where_ctid(self, mid_ctid1, start_ctid, view, tab):
        pass

    @abstractmethod
    def create_table_as_select_star_from_ctid(self, end_ctid, start_ctid, tab, fromtab):
        pass

    @abstractmethod
    def get_ctid_from(self, min_or_max, tabname):
        pass

    @abstractmethod
    def truncate_table(self, table):
        pass

    @abstractmethod
    def insert_into_tab_attribs_format(self, att_order, esc_string, tab):
        pass

    @abstractmethod
    def update_tab_attrib_with_value(self, tab, attrib, value):
        pass

    @abstractmethod
    def update_tab_attrib_with_quoted_value(self, tab, attrib, value):
        pass

    @abstractmethod
    def update_sql_query_tab_attribs(self, tab, attrib):
        pass

    def update_sql_query_tab_date_attrib_value(self, tab, attrib, value):
        return self.update_tab_attrib_with_value(tab, attrib, value)

    @abstractmethod
    def form_update_query_with_value(self, update_string, datatype, val):
        pass

    @abstractmethod
    def get_column_details_for_table(self, schema, tab):
        pass

    @abstractmethod
    def select_attribs_from_relation(self, tab_attribs, relation):
        attribs = ", ".join(tab_attribs)
        return f"select {attribs} from {relation};"

    @abstractmethod
    def insert_into_tab_select_star_fromtab(self, tab, fromtab):
        return f"Insert into {tab} Select * from {fromtab};"

    @abstractmethod
    def insert_into_tab_select_star_fromtab_with_ctid(self, tab, fromtab, ctid):
        return f"Insert into {tab} (Select * from {fromtab} Where ctid = {ctid});"

    @abstractmethod
    def select_ctid_from_tabname_offset(self, tabname, offset):
        return f"Select ctid from {tabname} offset {offset} Limit 1;"

    @abstractmethod
    def select_next_ctid(self, tabname, mid_ctid1):
        return f"Select Min(ctid) from {tabname} Where ctid > '{mid_ctid1}';"

    @abstractmethod
    def select_previous_ctid(self, tab, ctid1):
        return f"Select MAX(ctid) from {tab} Where ctid < '{ctid1}';"

    @abstractmethod
    def select_max_ctid(self, tab):
        return f"Select MAX(ctid) from {tab};"

    @abstractmethod
    def select_start_ctid_of_any_table(self):
        return '(0,1)'

    @abstractmethod
    def hashtext_query(self, tab):
        pass

    @abstractmethod
    def create_table_as_select_star_from_limit_1(self, tab, fromtab):
        pass
