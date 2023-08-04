from .ExtractorBase import Base
from ...refactored.executable import Executable
from ...refactored.util.utils import get_escape_string


class AfterWhereClauseBase(Base):

    def __init__(self, connectionHelper,
                 name,
                 core_relations,
                 global_all_attribs,
                 global_attrib_types,
                 join_graph,
                 filter_predicates):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)

        self.core_relations = core_relations
        self.global_all_attribs = global_all_attribs
        self.global_attrib_types = global_attrib_types
        self.global_join_graph = join_graph
        self.global_filter_predicates = filter_predicates

    def extract_params_from_args(self, args):
        return args[0]

    def construct_filter_attribs_dict(self):
        # get filter values and their allowed minimum and maximum value
        filter_attrib_dict = {}
        for entry in self.global_filter_predicates:
            if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
                filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
            else:
                filter_attrib_dict[(entry[0], entry[1])] = entry[3]
        return filter_attrib_dict

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        filter_attrib_dict = self.construct_filter_attribs_dict()
        check = self.doExtractJob(query, attrib_types_dict, filter_attrib_dict)
        return check

    def truncate_core_relations(self):
        # Truncate all core relations
        for table in self.core_relations:
            self.connectionHelper.execute_sql(["Truncate Table " + table + ";"])

    def insert_attrib_vals_into_table(self, att_order, attrib_list_inner, insert_rows, tabname_inner):
        att_order, esc_string = get_escape_string(att_order, attrib_list_inner)
        insert_query = "INSERT INTO " + tabname_inner + att_order + " VALUES " + esc_string
        self.connectionHelper.execute_sql_with_params(insert_query, insert_rows)

    def doExtractJob(self, query, attrib_types_dict, filter_attrib_dict):
        return True
