import copy
from datetime import date
from decimal import Decimal
from typing import Union, List, Tuple

from .abstract.abstractConnection import AbstractConnectionHelper
from .abstract.filter_holder import FilterHolder
from ..util.aoa_utils import get_min, get_max, get_attrib, get_tab, get_UB, get_LB, \
    get_delta, \
    get_all_two_combs, \
    optimize_edge_set, create_adjacency_map_from_aoa_predicates, find_all_chains, \
    add_concrete_bounds_as_edge2, remove_item_from_list, \
    find_le_attribs_from_edge_set, find_ge_attribs_from_edge_set, add_item_to_list, remove_absorbed_Bs, \
    find_transitive_concrete_upperBs, find_transitive_concrete_lowerBs, need_permanent_mutation, \
    find_concrete_bound_from_filter_bounds, add_item_to_dict, get_op, remove_item_from_dict
from ..util.utils import get_val_plus_delta, add_two, get_mid_val


def check_redundancy(fl_list, a_ineq):
    for pred in fl_list:
        if get_tab(a_ineq) == get_tab(pred) \
                and get_attrib(a_ineq) == get_attrib(pred) \
                and get_UB(a_ineq) == get_UB(pred) \
                and get_LB(a_ineq) == get_LB(pred):
            return True
    return False


class InequalityPredicate(FilterHolder):

    def __init__(self, connectionHelper: AbstractConnectionHelper,
                 core_relations: List[str],
                 pending_predicates, arithmetic_eq_predicates, algebraic_eq_predicates,
                 filter_extractor, global_min_instance_dict: dict):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, filter_extractor,
                         "InequalityPredicate")
        self.__absorbed_LBs = {}
        self.__absorbed_UBs = {}
        self.__ineaoa_enabled = True
        self.arithmetic_eq_predicates = arithmetic_eq_predicates
        self.algebraic_eq_predicates = algebraic_eq_predicates
        self.arithmetic_ineq_predicates = copy.deepcopy(pending_predicates)
        self.aoa_predicates = []
        self.aoa_less_thans = []
        self.arithmetic_filters = []
        self.__handle_filter_for_subrange = self.filter_extractor.handle_filter_for_subrange

    def doActualJob(self, args=None):
        query = super().doActualJob(args)
        self.restore_d_min_from_dict()
        if self.__ineaoa_enabled:
            self.__extract_aoa_core(query)
            self.__cleanup_predicates()
        self.__fill_in_internal_predicates()
        self.restore_d_min_from_dict()
        return True

    def __extract_aoa_core(self, query):
        edge_set_dict = self.__algo4_create_edgeSet_E()
        self.logger.debug("edge_set_dict:", edge_set_dict)
        for datatype in edge_set_dict.keys():
            E, L = self.algo7_find_aoa(edge_set_dict, datatype, query)
            self.logger.debug("E: ", E)
            self.logger.debug("L: ", L)
            self.aoa_predicates.extend(E)
            self.aoa_less_thans.extend(L)

    def __cleanup_predicates(self):
        self.__remove_arithmetic_eqs_from_aoa(self.aoa_predicates)
        self.__remove_transitive_concrete_bounds(self.aoa_predicates, self.aoa_less_thans)

    def __remove_arithmetic_eqs_from_aoa(self, edge_set):
        to_remove = []
        for aoa in edge_set:
            if not isinstance(aoa[0], tuple) and isinstance(aoa[1], tuple):
                check_eq_tuple = (aoa[1][0], aoa[1][1], "=", aoa[0], aoa[0])
                if check_eq_tuple in self.arithmetic_eq_predicates:
                    to_remove.append(aoa)
            elif isinstance(aoa[0], tuple) and not isinstance(aoa[1], tuple):
                check_eq_tuple = (aoa[0][0], aoa[0][1], "=", aoa[1], aoa[1])
                if check_eq_tuple in self.arithmetic_eq_predicates:
                    to_remove.append(aoa)

        eq_set = set()
        for aoa in edge_set:
            if not isinstance(aoa[0], tuple) and isinstance(aoa[1], tuple) and (aoa[1], aoa[0]) in edge_set:
                to_remove.append(aoa)
                to_remove.append((aoa[1], aoa[0]))
                eq_set.add((aoa[1][0], aoa[1][1], "=", aoa[0], aoa[0]))

        for t_r in to_remove:
            remove_item_from_list(t_r, edge_set)
        for e in eq_set:
            add_item_to_list(e, self.arithmetic_eq_predicates)

    def algo7_find_aoa(self, edge_set_dict: dict, datatype: str, query: str) -> Tuple[List, List]:
        edge_set = edge_set_dict[datatype]
        E, L = edge_set, []
        directed_paths = find_all_chains(create_adjacency_map_from_aoa_predicates(E))
        self.logger.debug("E: ", E)
        for path in directed_paths:
            for i, col_src in enumerate(path):
                col_sink = path[i + 1] if i + 1 < len(path) else None
                if col_sink is not None:
                    aoa = self.absorb_variable_LBs(E, L, datatype, col_src, col_sink, query)
                    self.absorb_variable_UBs(E, L, datatype, col_src, col_sink, query, aoa)
                    remove_absorbed_Bs(E, self.__absorbed_LBs, self.__absorbed_UBs, col_sink, col_src)
                    self.logger.debug("within chain E: ", E)

                self.__extract_dormant_LBs(E, col_src, datatype, query, L)
                self.logger.debug("after absorbing all bounds E: ", E)

        self.__revert_mutation_on_filter_global_min_instance_dict()
        self.__extract_dormant_UBs(E, datatype, directed_paths, query, L)
        self.logger.debug("after full round E: ", E)

        self.absorb_arithmetic_filters()  # mandatory cleanup
        self.__revert_mutation_on_filter_global_min_instance_dict()
        return E, L

    def absorb_arithmetic_filters(self):
        to_remove = []
        for eq in self.arithmetic_eq_predicates:
            tab, attrib, val = get_tab(eq), get_attrib(eq), eq[-1]
            if (tab, attrib) in self.__absorbed_LBs and val in self.__absorbed_LBs[(tab, attrib)] \
                    and (tab, attrib) in self.__absorbed_UBs and val in self.__absorbed_UBs[(tab, attrib)]:
                to_remove.append(eq)
        for t_r in to_remove:
            self.arithmetic_eq_predicates.remove(t_r)
        to_remove = []
        self.logger.debug(self.__absorbed_LBs)
        self.logger.debug(self.__absorbed_UBs)
        for eq in self.arithmetic_ineq_predicates:
            tab, attrib, lb, ub = get_tab(eq), get_attrib(eq), get_LB(eq), get_UB(eq)
            self.logger.debug(f"{tab}.{attrib}: LB {lb}, UB {ub}")
            if ((tab, attrib) in self.__absorbed_LBs and lb in self.__absorbed_LBs[(tab, attrib)]) \
                    or ((tab, attrib) in self.__absorbed_UBs and ub in self.__absorbed_UBs[(tab, attrib)]):
                to_remove.append(eq)
        for t_r in to_remove:
            self.arithmetic_ineq_predicates.remove(t_r)

    def __remove_transitive_concrete_bounds(self, E, L):
        to_remove = []
        find_transitive_concrete_upperBs(E, to_remove)
        find_transitive_concrete_lowerBs(E, to_remove)

        cb_list_with_nones = map(lambda x: x if (not isinstance(x[0], tuple) or not isinstance(x[1], tuple))
        else None, E)
        cb_list = list(filter(lambda ub: ub is not None, cb_list_with_nones))

        self.find_transitive_concrete_L_Bounds(L, cb_list, to_remove, is_UB=True)
        self.find_transitive_concrete_L_Bounds(L, cb_list, to_remove, is_UB=False)

        for t_r in to_remove:
            remove_item_from_list(t_r, E)

    def find_transitive_concrete_L_Bounds(self, L: List[Tuple], cb_list: list, to_remove: list, is_UB: bool) -> None:
        if not len(L) or not len(cb_list):
            return
        for edge in L:
            datatype = self.get_datatype(edge[0])
            coeff = -1 if is_UB else 1
            lup, gup = None, None
            lup = find_concrete_bound_from_filter_bounds(edge[0], cb_list, lup, is_upper_bound=is_UB)
            gup = find_concrete_bound_from_filter_bounds(edge[1], cb_list, gup, is_upper_bound=is_UB)
            base_b = gup if is_UB else lup
            other_b = lup if is_UB else gup
            if gup is not None and lup is not None:
                base_op_one = get_val_plus_delta(datatype, base_b, coeff * get_delta(self.constants_dict[datatype]))
                if other_b == base_op_one:
                    redundant_pred = (edge[0], lup) if is_UB else (gup, edge[1])
                    to_remove.append(redundant_pred)

    def find_concrete_bound_from_edge_set(self, attrib, edge_set, datatype, is_UB):
        col_ps_getter = find_ge_attribs_from_edge_set if is_UB else find_le_attribs_from_edge_set
        prev_b_none_getter = get_max if is_UB else get_min
        prev_b = None
        prev_b = find_concrete_bound_from_filter_bounds(attrib, edge_set, prev_b, is_UB)
        if prev_b is None:
            col_ps = col_ps_getter(attrib, edge_set)
            for col_p in col_ps:
                prev_b = self.get_dmin_val(get_attrib(col_p), get_tab(col_p))
        if prev_b is None:
            prev_b = prev_b_none_getter(self.constants_dict[datatype])
        if is_UB:
            self.logger.debug(f"UB of {attrib}: {prev_b}")
        else:
            self.logger.debug(f"LB of {attrib}: {prev_b}")
        return prev_b

    def absorb_variable_UBs(self, E, L, datatype, col_src, col_sink, query,
                            aoa_confirm):
        joined_src = self.__get_equi_join_group(col_src)
        prev_ub = self.find_concrete_bound_from_edge_set(col_src, E, datatype, True)
        if (col_src, col_sink) in E or (col_src, col_sink) in L:
            for _src in joined_src:
                add_item_to_dict(self.__absorbed_UBs, _src, prev_ub)

        col_sink_lb = self.find_concrete_bound_from_edge_set(col_sink, E, datatype, False)
        val, dmin_val = self.__mutate_attrib_with_Bound_val(col_sink, datatype, col_sink_lb, False, query)

        if val != dmin_val:
            new_ub_fe = self.__do_bound_check_again(col_src, datatype, query)
            new_ub = get_UB(new_ub_fe[0]) if new_ub_fe is not None and len(new_ub_fe) \
                else get_max(self.constants_dict[datatype])

            if prev_ub != new_ub:
                add_item_to_list((col_src, col_sink), E)
                for _src in joined_src:
                    add_item_to_dict(self.__absorbed_UBs, _src, prev_ub)

                if new_ub < val:
                    remove_item_from_list((col_src, col_sink), E)
                    add_item_to_list((col_src, col_sink), L)
            else:
                if not aoa_confirm:
                    self.logger.debug(f"no aoa due to fixed UB")
                    remove_item_from_list((col_src, col_sink), E)
                    joined_sink = self.__get_equi_join_group(col_sink)
                    for col in joined_sink:
                        self.mutate_dmin_with_val(datatype, col, dmin_val)
        else:
            if not aoa_confirm:
                self.logger.debug(f"no aoa due to immutability of dmin val")
                remove_item_from_list((col_src, col_sink), E)

    def absorb_variable_LBs(self, E, L, datatype, col_src, col_sink, query) -> bool:
        aoa_confirm = False
        joined_sink = self.__get_equi_join_group(col_sink)
        """
        lb is lesser than current d_min value
        if any mutation happens in d_min, make sure the lb is updated accordingly
        """
        prev_lb = self.find_concrete_bound_from_edge_set(col_sink, E, datatype, False)

        if (col_src, col_sink) in E or (col_src, col_sink) in L:
            for _sink in joined_sink:
                add_item_to_dict(self.__absorbed_LBs, _sink, prev_lb)

        col_src_ub = self.find_concrete_bound_from_edge_set(col_src, E, datatype, True)
        val, dmin_val = self.__mutate_attrib_with_Bound_val(col_src, datatype, col_src_ub, True, query)

        if val != dmin_val:
            new_lb_fe = self.__do_bound_check_again(col_sink, datatype, query)
            new_lb = get_LB(new_lb_fe[0]) if new_lb_fe is not None and len(new_lb_fe) \
                else get_min(self.constants_dict[datatype])

            if prev_lb != new_lb:
                aoa_confirm = True
                if new_lb > val:
                    remove_item_from_list((col_src, col_sink), E)
                    add_item_to_list((col_src, col_sink), L)
                """
                col_src now needs to be mutated with its LB. for extract_dormant_LBs next.
                so, the bounds need to be adjusted accordingly.
                Set E needs to have that updated bounds.
                """
                self.__mutate_col_to_lb_and_update_setE(col_src, datatype, query)
            else:
                self.logger.debug(f"no aoa due to fixed LB")
                remove_item_from_list((col_src, col_sink), E)
        else:
            self.logger.debug(f"no aoa due to immutability of dmin val")
            remove_item_from_list((col_src, col_sink), E)

        if not aoa_confirm:
            for _sink in joined_sink:
                remove_item_from_dict(self.__absorbed_LBs, _sink, prev_lb)

        return aoa_confirm

    def __mutate_col_to_lb_and_update_setE(self, col_src, datatype, query):
        mutation_lb_fe = self.__do_bound_check_again(col_src, datatype, query)
        mutation_lb = get_LB(mutation_lb_fe[0]) if mutation_lb_fe is not None and len(mutation_lb_fe) \
            else get_min(self.constants_dict[datatype])
        joined_src = self.__get_equi_join_group(col_src)
        for col in joined_src:
            self.mutate_dmin_with_val(datatype, col, mutation_lb)

    def __extract_dormant_LBs(self, E, col_src, datatype, query, L):
        lb_dot = self.__mutate_with_boundary_value(E, datatype, query, col_src, False)
        min_val = self.__what_is_possible_min_val(E, L, col_src, datatype)
        check = lb_dot not in [min_val, get_min(self.constants_dict[datatype])]
        if check:
            add_item_to_list((lb_dot, col_src), E)

    def __what_is_possible_min_val(self, E, L, col_src, datatype):
        min_val = None
        col_prev_list = find_le_attribs_from_edge_set(col_src, E)
        if len(col_prev_list):
            col_prev = col_prev_list[0]
            min_val = self.__what_is_possible_min_val(E, L, col_prev, datatype)
        else:
            col_prev_list = find_le_attribs_from_edge_set(col_src, L)
            if len(col_prev_list):
                col_prev = col_prev_list[0]
                dmin_col_prev = self.__what_is_possible_min_val(E, L, col_prev, datatype)
                min_val = get_val_plus_delta(datatype, dmin_col_prev, 1 * get_delta(self.constants_dict[datatype]))
        if min_val is None:
            min_val = get_min(self.constants_dict[datatype])
        return min_val

    def __what_is_possible_max_val(self, E, L, col_src, datatype):
        max_val = None
        col_next_list = find_ge_attribs_from_edge_set(col_src, E)
        if len(col_next_list):
            col_next = col_next_list[0]
            max_val = self.__what_is_possible_max_val(E, L, col_next, datatype)
        else:
            col_next_list = find_ge_attribs_from_edge_set(col_src, L)
            if len(col_next_list):
                col_next = col_next_list[0]
                dmin_col_next = self.__what_is_possible_max_val(E, L, col_next, datatype)  # self.get_dmin_val(
                max_val = get_val_plus_delta(datatype, dmin_col_next, -1 * get_delta(self.constants_dict[datatype]))
        if max_val is None:
            max_val = get_max(self.constants_dict[datatype])
        return max_val

    def __extract_dormant_UBs(self, E, datatype, directed_paths, query, L):
        for path in directed_paths:
            for i in reversed(range(len(path))):
                col_i = path[i]
                ub_dot = self.__mutate_with_boundary_value(E, datatype, query, col_i, True)
                max_val = self.__what_is_possible_max_val(E, L, col_i, datatype)
                check = ub_dot not in [max_val, get_max(self.constants_dict[datatype])]
                if check:
                    add_item_to_list((col_i, ub_dot), E)

    def __algo4_create_edgeSet_E(self) -> dict:
        filtered_dict = self.__isolate_ineq_aoa_preds_per_datatype()
        edge_set_dict = {}
        for datatype in filtered_dict:
            edge_set = []
            ineq_group = filtered_dict[datatype]
            self.__create_dashed_edges(ineq_group, edge_set)
            optimize_edge_set(edge_set)
            add_concrete_bounds_as_edge2(ineq_group, edge_set, datatype)
            edge_set_dict[datatype] = edge_set
        return edge_set_dict

    def do_permanent_mutation(self):
        directed_paths = find_all_chains(create_adjacency_map_from_aoa_predicates(self.aoa_less_thans))
        if not len(directed_paths):
            return
        for path in directed_paths:
            num, datatype = len(path), self.get_datatype(path[0])
            dmin_vals = [self.get_dmin_val(get_attrib(tab_attrib), get_tab(tab_attrib)) for tab_attrib in path]
            diffs = [dmin_vals[i + 1] - dmin_vals[i] for i in range(len(dmin_vals) - 1)]
            if need_permanent_mutation(datatype, diffs):
                self.logger.debug("Need to mutate d_min permanently!")
                lb = find_concrete_bound_from_filter_bounds(path[0], self.aoa_predicates, None, False)
                if lb is None:
                    lb = self.__what_is_possible_min_val(self.aoa_predicates,
                                                         self.aoa_less_thans, path[0], datatype)
                ub = find_concrete_bound_from_filter_bounds(path[-1], self.aoa_predicates, None, True)
                if ub is None:
                    ub = self.__what_is_possible_max_val(self.aoa_predicates,
                                                         self.aoa_less_thans, path[-1], datatype)
                self.logger.debug(f"min: {path[0]} {lb}, max: {path[-1]} {ub}")
                chunk_size = get_mid_val(datatype, ub, lb, num)
                new_vals = [lb]
                for i in range(1, num):
                    new_vals.append(add_two(copy.deepcopy(new_vals[-1]), chunk_size, datatype))

                for i in range(num):
                    min_val = find_concrete_bound_from_filter_bounds(path[i], self.aoa_predicates, None, False)
                    if min_val is None:
                        min_val = self.__what_is_possible_min_val(self.aoa_predicates,
                                                                  self.aoa_less_thans, path[i], datatype)
                    if min_val > new_vals[i]:
                        new_vals[i] = min_val
                self.logger.debug(new_vals)
                for i in range(num):
                    self.mutate_dmin_with_val(datatype, path[i], new_vals[i])
        self.global_min_instance_dict = copy.deepcopy(self.filter_extractor.global_min_instance_dict)
        # whose d min dict getting mutation? callers or aoa?

    def __fill_in_internal_predicates(self):
        for a_eq in self.arithmetic_eq_predicates:
            self.arithmetic_filters.append(a_eq)
        to_remove = []
        for a_ineq in self.arithmetic_ineq_predicates:
            red = check_redundancy(self.arithmetic_filters, a_ineq)
            if red:
                to_remove.append(a_ineq)
            else:
                self.arithmetic_filters.append(a_ineq)
        for t_r in to_remove:
            self.arithmetic_ineq_predicates.remove(t_r)

    def __get_equi_join_group(self, tab_attrib: Tuple[str, str]) -> List[Tuple[str, str]]:
        for eq in self.algebraic_eq_predicates:
            if tab_attrib in eq:
                var_eq = [e for e in eq if len(e) == 2]
                return var_eq
        return [tab_attrib]

    def __get_bound_when_tab_attrib_is_in_aoaChain(self, datatype, filter_attrib, is_UB):
        i_min, i_max = get_min(self.constants_dict[datatype]), get_max(self.constants_dict[datatype])
        if not len(filter_attrib):
            val = i_max if is_UB else i_min
        else:
            val = get_UB(filter_attrib[0]) if is_UB else get_LB(filter_attrib[0])
        return val

    def __mutate_with_boundary_value(self, edge_set, datatype, query, tab_attrib, is_UB):
        filter_attribs = []
        joined_tab_attrib = self.__get_equi_join_group(tab_attrib)
        min_val, max_val = self.__get_min_max_for_chain_bounds(datatype, tab_attrib, is_UB)
        self.__handle_filter_for_subrange(joined_tab_attrib, datatype, filter_attribs, max_val, min_val, query)
        val = self.__get_bound_when_tab_attrib_is_in_aoaChain(datatype, filter_attribs, is_UB)

        if val is None:
            for key in joined_tab_attrib:
                val = self.find_concrete_bound_from_edge_set(key, edge_set, datatype, is_UB)

        for key in joined_tab_attrib:
            tab, attrib = get_tab(key), get_attrib(key)
            self.mutate_dmin_with_val(self.get_datatype((tab, attrib)), (tab, attrib), val)
        return val

    def __revert_mutation_on_filter_global_min_instance_dict(self) -> None:
        self.filter_extractor.global_min_instance_dict = copy.deepcopy(self.global_min_instance_dict)

    def __do_bound_check_again(self, tab_attrib: Tuple[str, str], datatype: str, query: str) -> list:
        joined_attribs = self.__get_equi_join_group(tab_attrib)
        filter_attribs = []
        filter_attribs = self.__extract_filter_on_attrib_set(filter_attribs, query, joined_attribs, datatype)
        return filter_attribs

    def __extract_filter_on_attrib_set(self, filter_attribs, query, joined_attribs, datatype):
        self._extract_filter_on_attrib_set(filter_attribs, query, joined_attribs, datatype)
        self.logger.debug("filter attribs: ", filter_attribs)
        dec_filter_attribs = []
        for fl in filter_attribs:
            dec_filter_attribs.append((get_tab(fl), get_attrib(fl), get_op(fl), get_LB(fl), get_UB(fl)))
        filter_attribs = dec_filter_attribs
        return filter_attribs

    def __is_dmin_val_leq_LB(self, myself, other) -> bool:
        val = self.get_dmin_val(get_attrib(myself), get_tab(myself))
        other_lb = get_LB(other)
        satisfied = val <= other_lb
        self.logger.debug(
            f"dmin.{get_attrib(myself)}: {val}, LB.{get_attrib(other)}: {other_lb}, val <= other_lb: {satisfied}")
        return satisfied

    def __create_dashed_edges(self, ineq_group, edge_set) -> None:
        seq = get_all_two_combs(ineq_group)
        for e in seq:
            one, two = e[0], e[1]
            self.__create_dashed_edge_from_oneTotwo(edge_set, one, two)
            self.__create_dashed_edge_from_oneTotwo(edge_set, two, one)

    def __create_dashed_edge_from_oneTotwo(self, edge_set, one, two) -> None:
        tab_attrib = (get_tab(one), get_attrib(one))
        next_tab_attrib = (get_tab(two), get_attrib(two))
        check = self.__is_dmin_val_leq_LB(two, one)
        if check:
            edge_set.append(tuple([next_tab_attrib, tab_attrib]))

    def __isolate_ineq_aoa_preds_per_datatype(self) -> dict:
        datatype_dict = {}
        for a_eq in self.arithmetic_eq_predicates:
            datatype = self.get_datatype((get_tab(a_eq), get_attrib(a_eq)))
            if datatype != 'str':
                new_tup = (get_tab(a_eq), get_attrib(a_eq), 'range', get_LB(a_eq), get_UB(a_eq))
                self.arithmetic_ineq_predicates.append(new_tup)

        for pred in self.arithmetic_ineq_predicates:
            tab_attrib = (pred[0], pred[1])
            datatype = self.get_datatype(tab_attrib)
            if datatype in datatype_dict.keys():
                datatype_dict[datatype].append(pred)
            else:
                datatype_dict[datatype] = [pred]
        filtered_dict = {key: value for key, value in datatype_dict.items() if key != 'str' and len(value) > 1}
        for key in datatype_dict:
            if len(datatype_dict[key]) > 1:
                for pred in datatype_dict[key]:
                    self.arithmetic_ineq_predicates.remove(pred)
        return filtered_dict

    def __mutate_attrib_with_Bound_val(self, tab_attrib: Tuple[str, str], datatype: str, val: any,
                                       with_UB: bool, query: str) \
            -> Tuple[Union[int, Decimal, date], Union[int, Decimal, date]]:
        dmin_val = self.get_dmin_val(get_attrib(tab_attrib), get_tab(tab_attrib))
        factor = -1 if with_UB else 1
        if dmin_val == val:
            val = get_val_plus_delta(datatype, val, factor * get_delta(self.constants_dict[datatype]))
        joined_tab_attribs = self.__get_equi_join_group(tab_attrib)
        for t_a in joined_tab_attribs:
            self.mutate_dmin_with_val(datatype, t_a, val)
        new_res = self.app.doJob(query)
        if self.app.isQ_result_no_full_nullfree_row(new_res):
            for t_a in joined_tab_attribs:
                self.mutate_dmin_with_val(datatype, t_a, dmin_val)
                val = dmin_val
        return val, dmin_val

    def __get_min_max_for_chain_bounds(self, datatype, tab_attrib, is_UB):
        i_min, i_max = get_min(self.constants_dict[datatype]), get_max(self.constants_dict[datatype])
        if is_UB:
            min_val = self.__absorbed_UBs[tab_attrib][0] if tab_attrib in self.__absorbed_UBs.keys() else i_min
            max_val = i_max
        else:
            max_val = self.__absorbed_LBs[tab_attrib][0] if tab_attrib in self.__absorbed_LBs.keys() else i_max
            min_val = i_min
        return min_val, max_val
