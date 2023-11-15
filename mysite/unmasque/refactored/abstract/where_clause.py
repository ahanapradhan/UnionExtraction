import copy

from .MutationPipeLineBase import MutationPipeLineBase
from ..util.common_queries import get_column_details_for_table, select_attribs_from_relation
from ..util.utils import is_int
from ...src.core.abstract.dataclass.whereclause_data_class import WhereData


class WhereClause(MutationPipeLineBase, WhereData):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict, global_key_attributes=None):
        WhereData.__init__(self, global_key_lists, global_key_attributes)
        MutationPipeLineBase.__init__(self, connectionHelper, core_relations, global_min_instance_dict, "Where_clause")
        # init data
        self.global_d_plus_value = {}  # this is the tuple from D_min
        self.global_attrib_max_length = {}

        self.global_attrib_types_dict = {}
        self.global_attrib_dict = {}

    def get_init_data(self):
        if len(self.global_attrib_types) + len(self.global_all_attribs) + len(self.global_d_plus_value) + len(
                self.global_attrib_max_length) == 0:
            self.do_init()

    def do_init(self):
        for tabname in self.core_relations:
            tab_attribs = self.get_attrib_details(tabname)
            self.get_d_plus_values(tab_attribs, tabname)

    def get_attrib_details(self, tabname):
        res, desc = self.connectionHelper.execute_sql_fetchall(
            get_column_details_for_table(self.connectionHelper.config.schema, tabname))
        tab_attribs = []
        tab_attribs.extend(row[0] for row in res)
        self.global_all_attribs.append(copy.deepcopy(tab_attribs))
        self.global_attrib_types.extend((tabname, row[0], row[1]) for row in res)
        self.global_attrib_max_length.update(
            {(tabname, row[0]): int(str(row[2])) for row in res if is_int(str(row[2]))})
        return tab_attribs

    def get_d_plus_values(self, tab_attribs, tabname):
        res, desc = self.connectionHelper.execute_sql_fetchall(
            select_attribs_from_relation(tab_attribs, tabname))
        for row in res:
            for attrib, value in zip(tab_attribs, row):
                self.global_d_plus_value[attrib] = value

    def find_tabname_for_given_attrib(self, find_attrib):
        for entry in self.global_attrib_types:
            tabname = entry[0]
            attrib = entry[1]
            if attrib == find_attrib:
                return tabname
