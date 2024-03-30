from abc import ABC, abstractmethod

from ...refactored.util.utils import get_format


class CommonQueries(ABC):

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

    @abstractmethod
    def get_min_max_ctid(self, tab):
        pass

    @abstractmethod
    def drop_view(self, tab):
        pass

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
    def update_tab_attrib_with_value(self, attrib, tab, value):
        pass

    @abstractmethod
    def update_tab_attrib_with_quoted_value(self, tab, attrib, value):
        pass

    @abstractmethod
    def update_sql_query_tab_attribs(self, tab, attrib):
        pass

    def form_update_query_with_value(self, update_string, datatype, val):
        update_val = get_format(datatype, val)
        return f"{update_string} {update_val};"

    @abstractmethod
    def get_column_details_for_table(self, schema, tab):
        pass
