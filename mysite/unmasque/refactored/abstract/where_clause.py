import copy

from .MutationPipeLineBase import MutationPipeLineBase
from ..util.common_queries import get_column_details_for_table, select_attribs_from_relation, truncate_table
from ..util.utils import is_int


def parse_for_int(val):
    try:
        v_int = int(val)
        v_int = str(val)
    except ValueError:
        v_int = f"\'{str(val)}\'"
    except TypeError:
        v_int = f"\'{str(val)}\'"
    return v_int


class WhereClause(MutationPipeLineBase):

    def __init__(self, connectionHelper, global_key_lists, core_relations, global_min_instance_dict, name):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, name)
        self.global_key_lists = global_key_lists
        # init data
        self.global_attrib_types = []
        self.global_all_attribs = []
        self.global_d_plus_value = {}  # this is the tuple from D_min
        self.global_attrib_max_length = {}

        self.global_attrib_types_dict = {}
        self.global_attrib_dict = {}

    def revert_filter_changes(self, tabname):
        values = self.global_min_instance_dict[tabname]
        headers = values[0]
        comma_sep_h = ", ".join(headers)
        tuple_ = [parse_for_int(e) for e in values[1]]
        comma_sep_v = ", ".join(tuple_)
        ddl_ql = f"insert into {tabname}({comma_sep_h}) values({comma_sep_v});"
        self.connectionHelper.execute_sql([truncate_table(tabname),
                                           ddl_ql])

    def get_init_data(self):
        if len(self.global_attrib_types) + len(self.global_all_attribs) + len(self.global_d_plus_value) + len(
                self.global_attrib_max_length) == 0:
            self.do_init()

    def do_init(self):
        for tabname in self.core_relations:

            res, desc = self.connectionHelper.execute_sql_fetchall(
                get_column_details_for_table(self.connectionHelper.config.schema, tabname))

            tab_attribs = []
            tab_attribs.extend(row[0] for row in res)
            self.global_all_attribs.append(copy.deepcopy(tab_attribs))

            self.global_attrib_types.extend((tabname, row[0], row[1]) for row in res)

            self.global_attrib_max_length.update(
                {(tabname, row[0]): int(str(row[2])) for row in res if is_int(str(row[2]))})

            res, desc = self.connectionHelper.execute_sql_fetchall(
                select_attribs_from_relation(tab_attribs, tabname))
            for row in res:
                for attrib, value in zip(tab_attribs, row):
                    self.global_d_plus_value[attrib] = value
