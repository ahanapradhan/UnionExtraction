import copy

from ....src.core.abstract.MutationPipeLineBase import MutationPipeLineBase
from ....src.util.aoa_utils import get_tab, get_attrib, get_constants_for
from ....src.util.utils import get_format, get_min_and_max_val


class UN2WhereClause(MutationPipeLineBase):
    SUPPORTED_DATATYPES = ['int', 'date', 'numeric', 'number']
    TEXT_EQUALITY_OP = 'equal'
    MATH_EQUALITY_OP = '='

    def __init__(self, connectionHelper,
                 core_relations,
                 global_min_instance_dict, name):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, name)
        # init data
        self.global_attrib_types = []
        self.global_all_attribs = []
        self.global_d_plus_value = {}  # this is the tuple from D_min
        self.global_attrib_max_length = {}

        self.global_attrib_types_dict = {}
        self.global_attrib_dict = {}

        self.global_min_instance_dict_bkp = copy.deepcopy(self.global_min_instance_dict)
        self.constants_dict = {}
        self.init_done = False

    def doActualJob(self, args=None):
        query = self.extract_params_from_args(args)
        if not self.init_done:
            self.mock = self.mock
            self.init_constants()
            self.init_done = True
        return query

    def init_constants(self) -> None:
        for datatype in self.SUPPORTED_DATATYPES:
            i_min, i_max = get_min_and_max_val(datatype)
            delta, _ = get_constants_for(datatype)
            self.constants_dict[datatype] = (i_min, i_max, delta)

    def mutate_global_min_instance_dict(self, tab: str, attrib: str, val) -> None:
        g_min_dict = self.global_min_instance_dict
        data = g_min_dict[tab]
        idx = data[0].index(attrib)
        new_data = []
        for i in range(0, len(data[1])):
            if idx == i:
                new_data.append(val)
            else:
                new_data.append(data[1][i])
        data[1] = tuple(new_data)

    def restore_d_min_from_dict(self) -> None:
        self.global_min_instance_dict = copy.deepcopy(self.global_min_instance_dict_bkp)
        if not len(self.global_min_instance_dict):
            return
        for tab in self.core_relations:
            self.insert_into_dmin_dict_values(tab)

    def insert_into_dmin_dict_values(self, tabname):
        values = self.global_min_instance_dict[tabname]
        attribs, vals = values[0], values[1]
        attrib_list = ", ".join(attribs)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.truncate_table(tabname)])
        self.connectionHelper.execute_sql_with_params(
            self.connectionHelper.queries.insert_into_tab_attribs_format(f"({attrib_list})", "", tabname), [vals])

    def get_datatype(self, tab_attrib: tuple[str, str]) -> str:
        if any(x in self.global_attrib_types_dict[tab_attrib] for x in ['int', 'integer', 'number']):
            return 'int'
        elif 'date' in self.global_attrib_types_dict[tab_attrib]:
            return 'date'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['text', 'char', 'varbit', 'varchar2']):
            return 'str'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['numeric', 'float']):
            return 'numeric'
        else:
            raise ValueError

    def get_dmin_val_of_attrib_list(self, attrib_list: list) -> list:
        val_list = []
        for tab_attrib in attrib_list:
            tabname, attrib = tab_attrib[0], tab_attrib[1]
            val = self.get_dmin_val(attrib, tabname)
            val_list.append(val)
        return val_list

    def mutate_dmin_with_val(self, datatype, t_a, val):
        if datatype == 'date':
            self.connectionHelper.execute_sql(
                [self.connectionHelper.queries.update_sql_query_tab_date_attrib_value(get_tab(t_a),
                                                                                      get_attrib(
                                                                                          t_a),
                                                                                      get_format(
                                                                                          datatype,
                                                                                          val))], self.logger)
        else:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.update_tab_attrib_with_value(get_tab(t_a),
                                                                                                          get_attrib(
                                                                                                              t_a),
                                                                                                          get_format(
                                                                                                              datatype,
                                                                                                              val))],
                                              self.logger)
        self.mutate_global_min_instance_dict(get_tab(t_a), get_attrib(t_a), val)
        self.global_d_plus_value[get_attrib(t_a)] = val


