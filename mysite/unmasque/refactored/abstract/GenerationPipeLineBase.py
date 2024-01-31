import ast

from .MutationPipeLineBase import MutationPipeLineBase
from ..util.common_queries import insert_into_tab_attribs_format, update_tab_attrib_with_value, \
    update_tab_attrib_with_quoted_value
from ...refactored.util.utils import get_escape_string, get_dummy_val_for, get_format, get_char, get_unused_dummy_val, \
    get_min_and_max_val


def update_aoa_LB_UB(LB_dict, UB_dict, filter_attrib_dict):
    for attrib in LB_dict.keys():
        if attrib in UB_dict.keys():
            filter_attrib_dict[attrib] = (LB_dict[attrib], UB_dict[attrib])
            del LB_dict[attrib]
            del UB_dict[attrib]


def update_arithmetic_aoa_commons(LB_dict, UB_dict, filter_attrib_dict):
    for attrib in filter_attrib_dict:
        if attrib in LB_dict.keys() and filter_attrib_dict[attrib][0] < LB_dict[attrib]:
            filter_attrib_dict[attrib] = (LB_dict[attrib], filter_attrib_dict[attrib][1])
            del LB_dict[attrib]
        if attrib in UB_dict.keys() and \
                (len(filter_attrib_dict[attrib]) > 1 and filter_attrib_dict[attrib][1] > UB_dict[attrib])\
                or (len(filter_attrib_dict[attrib]) == 1 and filter_attrib_dict[attrib][0] > UB_dict[attrib]):
            filter_attrib_dict[attrib] = (filter_attrib_dict[attrib][0], UB_dict[attrib])
            del UB_dict[attrib]


class GenerationPipeLineBase(MutationPipeLineBase):

    def __init__(self, connectionHelper, name, core_relations, global_all_attribs, global_attrib_types, join_graph,
                 filter_predicates, global_min_instance_dict, global_key_attributes, aoa_predicates):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, name)
        self.global_all_attribs = global_all_attribs
        self.global_attrib_types = global_attrib_types
        self.global_join_graph = join_graph
        self.global_filter_predicates = filter_predicates
        self.filter_attrib_dict = {}
        self.attrib_types_dict = {}
        self.global_key_attributes = global_key_attributes
        self.global_aoa_predicates = aoa_predicates
        self.joined_attribs = None

    def extract_params_from_args(self, args):
        return args[0]

    def construct_filter_attribs_dict(self):
        # get filter values and their allowed minimum and maximum value
        filter_attrib_dict = {}
        self.add_arithmetic_filters(filter_attrib_dict)
        LB_dict, UB_dict = self.make_dmin_dict_from_aoa()
        update_arithmetic_aoa_commons(LB_dict, UB_dict, filter_attrib_dict)
        update_aoa_LB_UB(LB_dict, UB_dict, filter_attrib_dict)
        self.update_aoa_single_bounds(LB_dict, UB_dict, filter_attrib_dict)
        return filter_attrib_dict

    def update_aoa_single_bounds(self, LB_dict, UB_dict, filter_attrib_dict):
        to_del_LB = []
        for attrib in LB_dict.keys():
            datatype = self.attrib_types_dict[attrib]
            _, i_max = get_min_and_max_val(datatype)
            filter_attrib_dict[attrib] = (LB_dict[attrib], i_max)
            to_del_LB.append(attrib)
        for attrib in to_del_LB:
            del LB_dict[attrib]

        to_del_UB = []
        for attrib in UB_dict.keys():
            datatype = self.attrib_types_dict[attrib]
            i_min, _ = get_min_and_max_val(datatype)
            filter_attrib_dict[attrib] = (i_min, UB_dict[attrib])
            to_del_UB.append(attrib)
        for attrib in to_del_UB:
            del UB_dict[attrib]

    def add_arithmetic_filters(self, filter_attrib_dict):
        for entry in self.global_filter_predicates:
            if len(entry) > 4 and \
                    'like' not in entry[2].lower() and \
                    'equal' not in entry[2].lower():
                filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
            else:
                filter_attrib_dict[(entry[0], entry[1])] = entry[3]

    def make_dmin_dict_from_aoa(self):
        LB_dict, UB_dict = {}, {}
        for entry in self.global_aoa_predicates:
            l_attrib, r_attrib = entry[0], entry[1]
            if isinstance(l_attrib, tuple):
                l_dmin_val = self.get_dmin_val(l_attrib[1], l_attrib[0])
            else:
                l_dmin_val = l_attrib
            if isinstance(r_attrib, tuple):
                r_dmin_val = self.get_dmin_val(r_attrib[1], r_attrib[0])
            else:
                r_dmin_val = r_attrib

            if isinstance(r_attrib, tuple):
                if r_attrib not in LB_dict.keys():
                    LB_dict[r_attrib] = l_dmin_val
                else:
                    if l_dmin_val > LB_dict[r_attrib]:
                        LB_dict[r_attrib] = l_dmin_val
            if isinstance(l_attrib, tuple):
                if l_attrib not in UB_dict.keys():
                    UB_dict[l_attrib] = r_dmin_val
                else:
                    if r_dmin_val < UB_dict[l_attrib]:
                        UB_dict[l_attrib] = r_dmin_val
        return LB_dict, UB_dict

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.do_init()
        check = self.doExtractJob(query)
        return check

    def do_init(self):
        self.attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        self.filter_attrib_dict = self.construct_filter_attribs_dict()
        if self.global_join_graph is None:
            return
        C_E = set()
        for edge in self.global_join_graph:
            C_E.add(edge[0])
            C_E.add(edge[1])
        self.joined_attribs = list(C_E)

    def get_datatype(self, tab_attrib):
        if any(x in self.attrib_types_dict[tab_attrib] for x in ['int', 'integer']):
            return 'int'
        elif 'date' in self.attrib_types_dict[tab_attrib]:
            return 'date'
        elif any(x in self.attrib_types_dict[tab_attrib] for x in ['text', 'char', 'varbit']):
            return 'str'
        elif any(x in self.attrib_types_dict[tab_attrib] for x in ['numeric', 'float']):
            return 'numeric'
        else:
            raise ValueError

    def get_s_val_for_textType(self, attrib_inner, tabname_inner):
        filtered_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)]
        if isinstance(filtered_val, tuple):
            filtered_val = filtered_val[0]
        return filtered_val

    def insert_attrib_vals_into_table(self, att_order, attrib_list_inner, insert_rows, tabname_inner):
        esc_string = get_escape_string(attrib_list_inner)
        insert_query = insert_into_tab_attribs_format(att_order, esc_string, tabname_inner)
        self.connectionHelper.execute_sql_with_params(insert_query, insert_rows)

    def update_attrib_in_table(self, attrib, value, tabname):
        update_query = update_tab_attrib_with_value(attrib, tabname, value)
        self.connectionHelper.execute_sql([update_query])

    def doExtractJob(self, query):
        return True

    def update_with_val(self, attrib, tabname, val):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = update_tab_attrib_with_value(attrib, tabname, get_format('date', val))
        elif 'int' in self.attrib_types_dict[(tabname, attrib)] \
                or 'numeric' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = update_tab_attrib_with_value(attrib, tabname, val)
        else:
            update_q = update_tab_attrib_with_quoted_value(tabname, attrib, val)
        self.connectionHelper.execute_sql([update_q])

    def get_val(self, attrib, tabname):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for('date')
            val = ast.literal_eval(get_format('date', val))

        elif ('int' in self.attrib_types_dict[(tabname, attrib)]
              or 'numeric' in self.attrib_types_dict[(tabname, attrib)]):
            # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for('int')
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.filter_attrib_dict[(tabname, attrib)]
                self.logger.debug(val)
                val = val.replace('%', '')
            else:
                val = get_char(get_dummy_val_for('char'))
        return val

    def get_different_val_for_dmin(self, attrib, tabname, prev):
        if prev == self.filter_attrib_dict[(tabname, attrib)][0]:
            val = self.filter_attrib_dict[(tabname, attrib)][1]
        elif prev == self.filter_attrib_dict[(tabname, attrib)][1]:
            val = self.filter_attrib_dict[(tabname, attrib)][0]
        else:
            val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                      self.filter_attrib_dict[(tabname, attrib)][1])
        return val

    def get_different_val(self, attrib, tabname, prev):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_different_val_for_dmin(attrib, tabname, prev)
            else:
                val = get_unused_dummy_val('date', [prev])
            val = ast.literal_eval(get_format('date', val))

        elif ('int' in self.attrib_types_dict[(tabname, attrib)]
              or 'numeric' in self.attrib_types_dict[(tabname, attrib)]):
            # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_different_val_for_dmin(attrib, tabname, prev)
            else:
                val = get_unused_dummy_val('int', [prev])
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.filter_attrib_dict[(tabname, attrib)]
                self.logger.debug(val)
                val = val.replace('%', '')
            else:
                val = get_char(get_unused_dummy_val('char', [prev]))
        return val

    def find_tabname_for_given_attrib(self, find_attrib):
        for entry in self.global_attrib_types:
            tabname = entry[0]
            attrib = entry[1]
            if attrib == find_attrib:
                return tabname
