import copy

from .ExtractorBase import Base
from ..executable import Executable
from ..util.utils import is_int


class WhereClause(Base):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict):
        super().__init__(connectionHelper, "Where_clause")
        self.app = Executable(connectionHelper)

        # from initiator
        self.global_key_lists = global_key_lists

        # from from clause
        self.core_relations = core_relations

        # from view minimizer
        self.global_min_instance_dict = global_min_instance_dict

        # init data
        self.global_attrib_types = []
        self.global_all_attribs = []
        self.global_d_plus_value = {}  # this is the tuple from D_min
        self.global_attrib_max_length = {}

        self.global_attrib_types_dict = {}
        self.global_attrib_dict = {}

    def extract_params_from_args(self, args):
        return args[0]

    def get_init_data(self):
        if len(self.global_attrib_types) + len(self.global_all_attribs) + len(self.global_d_plus_value) + len(
                self.global_attrib_max_length) == 0:
            self.do_init()

    def do_init(self):
        for tabname in self.core_relations:
            res, desc = self.connectionHelper.execute_sql_fetchall("select column_name, data_type, "
                                                                   "character_maximum_length from "
                                                                   "information_schema.columns "
                                                                   "where table_schema = 'public' and "
                                                                   "table_name = '" + tabname + "';")
            tab_attribs = []
            tab_attribs.extend(row[0] for row in res)
            self.global_all_attribs.append(copy.deepcopy(tab_attribs))

            self.global_attrib_types.extend((tabname, row[0], row[1]) for row in res)

            self.global_attrib_max_length.update(
                {(tabname, row[0]): int(str(row[2])) for row in res if is_int(str(row[2]))})

            res, desc = self.connectionHelper.execute_sql_fetchall("select "
                                                                   + ", ".join(tab_attribs)
                                                                   + " from " + tabname + ";")
            for row in res:
                for attrib, value in zip(tab_attribs, row):
                    self.global_d_plus_value[attrib] = value

