import copy
from typing import List, Tuple

from frozenlist._frozenlist import FrozenList

from ..aoa import InequalityPredicate
from ..filter import Filter
from ...util.aoa_utils import get_constants_for
from ...util.error_codes import ERROR_007
from ...util.error_handling import UnmasqueError
from ...util.utils import get_val_plus_delta, get_min_and_max_val


def collect_attribs_from(_input):
    if _input is None:
        return []
    C_E = set()
    for edge in _input:
        C_E.add(edge[0])
        C_E.add(edge[1])
    output = list(C_E)
    return output


def update_aoa_LB_UB(LB_dict, UB_dict, filter_attrib_dict):
    to_del = []
    for attrib in LB_dict.keys():
        if attrib in UB_dict.keys():
            filter_attrib_dict[attrib] = (LB_dict[attrib], UB_dict[attrib])
            to_del.append(attrib)
    for attrib in to_del:
        del UB_dict[attrib]
        del LB_dict[attrib]


def update_arithmetic_aoa_commons(LB_dict, UB_dict, filter_attrib_dict):
    for attrib in filter_attrib_dict:
        if not isinstance(filter_attrib_dict[attrib], tuple):
            continue
        if len(filter_attrib_dict[attrib]) > 2:  # IN
            continue
        if attrib in LB_dict.keys():
            if filter_attrib_dict[attrib][0] < LB_dict[attrib]:
                filter_attrib_dict[attrib] = (LB_dict[attrib], filter_attrib_dict[attrib][1])
            del LB_dict[attrib]
        if attrib in UB_dict.keys():
            if (len(filter_attrib_dict[attrib]) > 1 and filter_attrib_dict[attrib][1] > UB_dict[attrib]) \
                    or (len(filter_attrib_dict[attrib]) == 1 and filter_attrib_dict[attrib][0] > UB_dict[attrib]):
                filter_attrib_dict[attrib] = (filter_attrib_dict[attrib][0], UB_dict[attrib])
            del UB_dict[attrib]


def sort_merge(range_values, single_values, values):
    single_values.sort()
    range_values.sort(key=lambda tup: tup[0])  # sorts in place
    s_counter, r_counter = 0, 0
    while s_counter < len(single_values) and r_counter < len(range_values):
        if single_values[s_counter] <= range_values[r_counter][0]:
            values.append(single_values[s_counter])
            s_counter += 1
        else:
            values.append(range_values[r_counter])
            r_counter += 1
    while s_counter < len(single_values):
        values.append(single_values[s_counter])
        s_counter += 1
    while r_counter < len(range_values):
        values.append(range_values[r_counter])
        r_counter += 1


def update_dict(B_dict, key, val, is_ub):
    if not isinstance(key, tuple):
        return
    if key in B_dict.keys():
        if is_ub:
            if val < B_dict[key]:
                B_dict[key] = val
        else:
            if val > B_dict[key]:
                B_dict[key] = val
    else:
        B_dict[key] = val


def update_transitive_bound(aoa_predicates, LB_dict, UB_dict, key, val, is_ub):
    if is_ub:
        update_dict(UB_dict, key, val, is_ub)
        '''
        for aoa in aoa_predicates:
            if aoa[0] == key:
                update_dict(LB_dict, aoa[1], val, not is_ub)
                break
        '''
    else:
        update_dict(LB_dict, key, val, is_ub)
        '''
        for aoa in aoa_predicates:
            if aoa[1] == key:
                update_dict(UB_dict, aoa[0], val, not is_ub)
                break
        '''


def update_LB_UB_dicts(aoa_preds, LB_dict, UB_dict, l_attrib, l_ub, r_attrib, r_lb):
    if isinstance(r_attrib, tuple):
        update_transitive_bound(aoa_preds, LB_dict=LB_dict, UB_dict=UB_dict, key=r_attrib, val=r_lb, is_ub=False)
    if isinstance(l_attrib, tuple):
        update_transitive_bound(aoa_preds, LB_dict=LB_dict, UB_dict=UB_dict, key=l_attrib, val=l_ub, is_ub=True)


class GenPipelineContext:

    def __init__(self, core_relations: List[str],
                 aoa_extractor: InequalityPredicate,
                 filter_extractor: Filter,
                 global_min_instance_dict: dict,
                 or_predicates):
        self.core_relations = core_relations
        self.global_all_attribs = filter_extractor.global_all_attribs
        self.attrib_types_dict = None
        self.filter_attrib_dict = {}
        self.joined_attribs = None
        self.__aoa_attribs = None
        self.arithmetic_filters = aoa_extractor.arithmetic_filters
        self.global_min_instance_dict = copy.deepcopy(global_min_instance_dict)
        self.__or_predicates = or_predicates
        self.filter_in_predicates = []

        self.__algebraic_eq_predicates = aoa_extractor.algebraic_eq_predicates
        self.global_join_graph = []
        self.global_attrib_types = filter_extractor.global_attrib_types
        self.global_aoa_le_predicates = aoa_extractor.aoa_predicates
        self.global_aoa_l_predicates = aoa_extractor.aoa_less_thans

        # methods passed by aoa extractor
        self.get_dmin_val = filter_extractor.get_dmin_val
        self.get_datatype = filter_extractor.get_datatype
        self.do_permanent_mutation = aoa_extractor.do_permanent_mutation
        self.restore_dmin_from_dict = aoa_extractor.restore_d_min_from_dict

    def doJob(self):
        self.restore_dmin_from_dict()
        self.__generate_arithmetic_conjunctive_disjunctions()
        g_mut_dict = self.do_permanent_mutation()
        self.global_min_instance_dict = copy.deepcopy(g_mut_dict)
        self.__create_equi_join_graph()
        self.attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        self.filter_attrib_dict = self.__construct_filter_attribs_dict()
        self.joined_attribs = collect_attribs_from(self.global_join_graph)
        self.__aoa_attribs = collect_attribs_from(self.global_aoa_le_predicates)
        self.__update_filter_predicates_from_filter_dict()

    def __create_equi_join_graph(self):
        for eq_join in self.__algebraic_eq_predicates:
            join_graph_edge = list(f"{item[1]}" for item in eq_join if len(item) == 2)
            join_graph_edge.sort()
            for i in range(0, len(join_graph_edge) - 1):
                self.global_join_graph.append([join_graph_edge[i], join_graph_edge[i + 1]])

    def __construct_filter_attribs_dict(self) -> dict:
        filter_attrib_dict, LB_dict, UB_dict = {}, {}, {}
        self.__add_arithmetic_filters(filter_attrib_dict)
        LB_dict, UB_dict = self.__make_dmin_dict_from_aoa_le(self.global_aoa_le_predicates, LB_dict, UB_dict)
        LB_dict, UB_dict = self.__make_dmin_dict_from_aoa_l(self.global_aoa_l_predicates, LB_dict, UB_dict)
        update_arithmetic_aoa_commons(LB_dict, UB_dict, filter_attrib_dict)
        update_aoa_LB_UB(LB_dict, UB_dict, filter_attrib_dict)
        self.__update_aoa_single_bounds(LB_dict, UB_dict, filter_attrib_dict)
        all_keys = list(filter_attrib_dict.keys())
        for entry in all_keys:
            self.__add_for_joined_attribs(entry, filter_attrib_dict)
        return filter_attrib_dict

    def __update_aoa_single_bounds(self, LB_dict: dict, UB_dict: dict, filter_attrib_dict):
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

    def __add_arithmetic_filters(self, filter_attrib_dict):
        for entry in self.arithmetic_filters:
            if len(entry) > 4:
                if entry[2].lower() not in ['like', 'equal', 'in']:  # <=, >=, =
                    filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
                if entry[2].lower() in ['like', 'equal']:
                    filter_attrib_dict[(entry[0], entry[1])] = entry[3]
        for entry in self.filter_in_predicates:
            filter_attrib_dict[(entry[0], entry[1])] = entry[3]

    def __add_for_joined_attribs(self, entry, filter_attrib_dict):
        for edge in self.__algebraic_eq_predicates:
            if entry in edge:
                others = [e for e in edge if len(e) == 2 and (e[0] != entry[0] or e[1] != entry[1])]
                for o in others:
                    filter_attrib_dict[o] = filter_attrib_dict[entry]
                break

    def __make_dmin_dict_from_aoa_le(self, global_aoa_le_predicates, LB_dict, UB_dict):
        for entry in global_aoa_le_predicates:
            l_attrib, r_attrib = entry[0], entry[1]
            if isinstance(l_attrib, tuple):
                l_dmin_val = self.get_dmin_val(l_attrib[1], l_attrib[0])
            else:
                l_dmin_val = l_attrib
            if isinstance(r_attrib, tuple):
                r_dmin_val = self.get_dmin_val(r_attrib[1], r_attrib[0])
            else:
                r_dmin_val = r_attrib

            update_LB_UB_dicts(global_aoa_le_predicates, LB_dict, UB_dict, l_attrib, l_dmin_val, r_attrib,
                               r_dmin_val)
        return LB_dict, UB_dict

    def __update_filter_predicates_from_filter_dict(self):
        filter_dict = []
        for pred in self.arithmetic_filters:
            key = (pred[0], pred[1])
            filter_dict.append(key)

        to_add = set()
        for key in self.filter_attrib_dict.keys():
            if key in filter_dict:
                continue
            if len(self.filter_attrib_dict[key]) > 2 or any(isinstance(v, tuple) for v in self.filter_attrib_dict[key]):
                continue
            bounds = self.filter_attrib_dict[key]
            datatype = self.get_datatype((key[0], key[1]))
            i_min, i_max = get_min_and_max_val(datatype)
            if not isinstance(bounds, tuple):  # single value
                op = '='
                lb, ub = bounds, bounds
            else:
                lb, ub = bounds[0], bounds[1]
                if lb == i_min:
                    op = '<='
                elif ub == i_max:
                    op = '>='
                else:
                    op = 'range'
            to_add.add((key[0], key[1], op, lb, ub))
        self.arithmetic_filters.extend(list(to_add))

    def __make_dmin_dict_from_aoa_l(self, global_aoa_l_predicates, LB_dict: dict, UB_dict: dict) -> Tuple[dict, dict]:
        for entry in global_aoa_l_predicates:
            datatype = self.get_datatype((entry[0]))
            delta, _ = get_constants_for(datatype)

            l_attrib, r_attrib = entry[0], entry[1]
            l_dmin_val = self.get_dmin_val(l_attrib[1], l_attrib[0])
            r_dmin_val = self.get_dmin_val(r_attrib[1], r_attrib[0])

            l_ub = get_val_plus_delta(datatype, r_dmin_val, -1 * delta)
            r_lb = get_val_plus_delta(datatype, l_dmin_val, 1 * delta)

            update_LB_UB_dicts(global_aoa_l_predicates, LB_dict, UB_dict, l_attrib, l_ub, r_attrib, r_lb)

        return LB_dict, UB_dict

    def __generate_arithmetic_conjunctive_disjunctions(self):
        for p in self.__or_predicates:
            non_empty_indices = [i for i, t_a in enumerate(p) if t_a]
            if len(non_empty_indices) == 1:
                if p[non_empty_indices[0]] not in self.arithmetic_filters:
                    self.arithmetic_filters.append(p[non_empty_indices[0]])
                continue
            tab_attribs = [(p[i][0], p[i][1]) for i in non_empty_indices]
            uniq_tab_attribs = set(tab_attribs)
            if len(uniq_tab_attribs) == 1:
                tab, attrib = next(iter(uniq_tab_attribs))
                proper_in_op = all(p[i][2] in ['equal', '='] for i in non_empty_indices)
                if proper_in_op:
                    values = [p[i][3] for i in non_empty_indices]
                    values.sort()
                else:
                    values = []
                    for i in non_empty_indices:
                        op = p[i][2]
                        if op == '=':
                            values.append(p[i][3])
                            continue
                        else:
                            i_min = p[i][3]
                            i_max = p[i][4]
                        values.append((i_min, i_max))
                self.__adjust_for_in_predicates(attrib, tab, values)
            else:
                raise UnmasqueError(ERROR_007, "genPipeline_context", "Disjunction on Multiple Attributes are out of scope!")

    def __adjust_for_in_predicates(self, attrib, tab, _values):
        values = []
        single_values = []
        range_values = []
        for val in _values:
            if not isinstance(val, tuple) and not isinstance(val, list):
                single_values.append(val)
            else:
                range_values.append(val)
        sort_merge(range_values, single_values, values)
        in_pred = [tab, attrib, 'IN', values, values] if len(values) > 1 else [tab, attrib, '=', values, values]
        frozen_values = FrozenList(values)
        frozen_values.freeze()
        frozen_in_pred = (in_pred[0], in_pred[1], in_pred[2], frozen_values, frozen_values)
        self.filter_in_predicates.append(frozen_in_pred)
        remove_filter_predicates = []
        for eq_pred in self.arithmetic_filters:
            if eq_pred[0] == tab and eq_pred[1] == attrib:
                remove_filter_predicates.append(eq_pred)
        for t_r in remove_filter_predicates:
            self.arithmetic_filters.remove(t_r)
