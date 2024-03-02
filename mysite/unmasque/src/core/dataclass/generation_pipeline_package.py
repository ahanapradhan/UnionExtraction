import copy
from datetime import date
from typing import Callable, Union

from mysite.unmasque.refactored.filter import get_constants_for
from mysite.unmasque.refactored.util.utils import get_min_and_max_val, get_val_plus_delta


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
        if attrib in LB_dict.keys() and filter_attrib_dict[attrib][0] < LB_dict[attrib]:
            filter_attrib_dict[attrib] = (LB_dict[attrib], filter_attrib_dict[attrib][1])
            del LB_dict[attrib]
        if attrib in UB_dict.keys() and \
                (len(filter_attrib_dict[attrib]) > 1 and filter_attrib_dict[attrib][1] > UB_dict[attrib]) \
                or (len(filter_attrib_dict[attrib]) == 1 and filter_attrib_dict[attrib][0] > UB_dict[attrib]):
            filter_attrib_dict[attrib] = (filter_attrib_dict[attrib][0], UB_dict[attrib])
            del UB_dict[attrib]


class PackageForGenPipeline:
    def __init__(self, core_relations: list[str],
                 global_all_attribs,
                 global_attrib_types,
                 global_filter_predicates: list[tuple[str, str, str,
                            Union[int, date, float],
                            Union[int, date, float]]],
                 global_aoa_le_predicates: list[tuple[str, str], tuple[str, str]],
                 global_join_graph,
                 global_aoa_l_predicates: list[tuple[str, str], tuple[str, str]],
                 global_min_instance_dict: dict,

                 get_dmin_val: Callable[[str, str], any],
                 get_datatype: Callable[[tuple[str, str]], str]):

        self.core_relations = core_relations
        self.global_all_attribs = global_all_attribs
        self.attrib_types_dict = None
        self.filter_attrib_dict = None
        self.joined_attribs = None
        self.aoa_attribs = None
        self.global_filter_predicates = global_filter_predicates
        self.global_min_instance_dict = copy.deepcopy(global_min_instance_dict)

        self.global_join_graph = global_join_graph
        self.global_attrib_types = global_attrib_types
        self.global_aoa_le_predicates = global_aoa_le_predicates
        self.global_aoa_l_predicates = global_aoa_l_predicates

        # methods passed by aoa extractor
        self.get_dmin_val = get_dmin_val
        self.get_datatype = get_datatype

    def doJob(self):
        print(self.global_min_instance_dict)
        self.attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        self.filter_attrib_dict = self.construct_filter_attribs_dict()
        self.joined_attribs = collect_attribs_from(self.global_join_graph)
        self.aoa_attribs = collect_attribs_from(self.global_aoa_le_predicates)
        self.update_filter_predicates()

    def construct_filter_attribs_dict(self) -> dict:
        # get filter values and their allowed minimum and maximum value
        filter_attrib_dict = {}
        self.add_arithmetic_filters(filter_attrib_dict)
        LB_dict, UB_dict = self.make_dmin_dict_from_aoa_le()
        LB_dict, UB_dict = self.make_dmin_dict_from_aoa_l(LB_dict, UB_dict)
        update_arithmetic_aoa_commons(LB_dict, UB_dict, filter_attrib_dict)
        update_aoa_LB_UB(LB_dict, UB_dict, filter_attrib_dict)
        self.update_aoa_single_bounds(LB_dict, UB_dict, filter_attrib_dict)
        return filter_attrib_dict

    def update_aoa_single_bounds(self, LB_dict: dict, UB_dict: dict, filter_attrib_dict: dict):
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

    def add_arithmetic_filters(self, filter_attrib_dict: dict):
        for entry in self.global_filter_predicates:
            if len(entry) > 4 and \
                    'like' not in entry[2].lower() and \
                    'equal' not in entry[2].lower():
                filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
            else:
                filter_attrib_dict[(entry[0], entry[1])] = entry[3]

    def make_dmin_dict_from_aoa_le(self) -> tuple[dict, dict]:
        LB_dict, UB_dict = {}, {}
        for entry in self.global_aoa_le_predicates:
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

    def update_filter_predicates(self):
        filter_dict = []
        for pred in self.global_filter_predicates:
            key = (pred[0], pred[1])
            filter_dict.append(key)

        to_add = set()
        for key in self.filter_attrib_dict.keys():
            if key in filter_dict:
                continue
            bounds = self.filter_attrib_dict[key]
            datatype = self.get_datatype((key[0], key[1]))
            if datatype == 'numeric':
                if not isinstance(bounds, tuple):  # single value
                    to_add.add((key[0], key[1], 'equal', float(bounds), float(bounds)))
                else:
                    to_add.add((key[0], key[1], 'range', float(bounds[0]), float(bounds[1])))
            else:
                if not isinstance(bounds, tuple):  # single value
                    to_add.add((key[0], key[1], 'equal', bounds, bounds))
                else:
                    to_add.add((key[0], key[1], 'range', bounds[0], bounds[1]))
        self.global_filter_predicates.extend(list(to_add))

    def make_dmin_dict_from_aoa_l(self, LB_dict: dict, UB_dict: dict) -> tuple[dict, dict]:
        for entry in self.global_aoa_l_predicates:
            datatype = self.get_datatype(entry[0])
            delta, _ = get_constants_for(datatype)

            l_attrib, r_attrib = entry[0], entry[1]
            l_dmin_val = self.get_dmin_val(l_attrib[1], l_attrib[0])
            r_dmin_val = self.get_dmin_val(r_attrib[1], r_attrib[0])

            r_lb = get_val_plus_delta(datatype, r_dmin_val, -1 * delta)
            if l_attrib not in UB_dict.keys():
                UB_dict[l_attrib] = r_lb
            else:
                if r_lb < UB_dict[l_attrib]:
                    UB_dict[l_attrib] = r_lb

            l_ub = get_val_plus_delta(datatype, l_dmin_val, 1 * delta)
            if r_attrib not in LB_dict.keys():
                LB_dict[r_attrib] = l_ub
            else:
                if l_ub > LB_dict[r_attrib]:
                    LB_dict[r_attrib] = l_ub

        return LB_dict, UB_dict
