import copy
from datetime import date
from typing import Union

from mysite.unmasque.refactored.abstract.MutationPipeLineBase import MutationPipeLineBase
from mysite.unmasque.refactored.filter import Filter, get_constants_for
from mysite.unmasque.refactored.util.common_queries import update_tab_attrib_with_value
from mysite.unmasque.refactored.util.utils import get_format, get_min_and_max_val, \
    get_val_plus_delta
from mysite.unmasque.src.core.QueryStringGenerator import handle_range_preds
from mysite.unmasque.src.core.dataclass.generation_pipeline_package import PackageForGenPipeline
from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper
from mysite.unmasque.src.util.aoa_utils import get_UB, get_attrib, get_max, get_delta, get_min, get_tab, get_LB, \
    get_min_max_for_chain_bounds, split_directed_path, add_pred_for, get_val_bound_for_chain, \
    add_concrete_bounds_as_edge, optimize_edge_set, create_adjacency_map_from_aoa_predicates, find_all_chains, \
    get_LB_of_next_attrib, adjust_Bounds2, left_over_aoa_CBs, get_all_two_combs, merge_equivalent_paritions, \
    get_out_edges, create_attrib_set_from_filter_predicates


class AlgebraicPredicate(MutationPipeLineBase):
    def __init__(self, connectionHelper: ConnectionHelper, core_relations: list[str], global_min_instance_dict: dict):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, "AlgebraicPredicate")
        self.filter_extractor = Filter(self.connectionHelper, core_relations, global_min_instance_dict)

        self.get_datatype = self.filter_extractor.get_datatype  # method

        self.pipeline_delivery = None

        self.aoa_predicates = []
        self.arithmetic_eq_predicates = []
        self.algebraic_eq_predicates = []
        self.arithmetic_ineq_predicates = []
        self.aoa_less_thans = []
        self.global_min_instance_dict_bkp = copy.deepcopy(global_min_instance_dict)

        self.where_clause = ""

        self.join_graph = []
        self.filter_predicates = []

        self.constants_dict = {}

    def init_constants(self):
        for datatype in ['date', 'int', 'numeric']:
            i_min, i_max = get_min_and_max_val(datatype)
            delta, _ = get_constants_for(datatype)
            self.constants_dict[datatype] = (i_min, i_max, delta)

    def post_process_for_generation_pipeline(self):
        self.global_min_instance_dict = copy.deepcopy(self.global_min_instance_dict_bkp)
        self.pipeline_delivery = PackageForGenPipeline(self.core_relations,
                                                       self.filter_extractor.global_all_attribs,
                                                       self.filter_extractor.global_attrib_types,
                                                       self.filter_predicates,
                                                       self.aoa_predicates,
                                                       self.join_graph,
                                                       self.aoa_less_thans,
                                                       self.global_min_instance_dict,
                                                       self.get_dmin_val,
                                                       self.get_datatype)
        self.pipeline_delivery.doJob()

    def generate_where_clause(self):
        predicates = []
        for eq_join in self.algebraic_eq_predicates:
            join_edge = list(item[1] for item in eq_join if len(item) == 2)
            join_edge.sort()
            for i in range(0, len(join_edge) - 1):
                join_e = f"{join_edge[i]} = {join_edge[i + 1]}"
                predicates.append(join_e)
                self.join_graph.append([join_edge[i], join_edge[i + 1]])
        for a_eq in self.arithmetic_eq_predicates:
            datatype = self.get_datatype((a_eq[0], a_eq[1]))
            pred = f"{a_eq[1]} = {get_format(datatype, a_eq[3])}"
            predicates.append(pred)
            self.filter_predicates.append(a_eq)
        for a_ineq in self.arithmetic_ineq_predicates:
            datatype = self.get_datatype((a_ineq[0], a_ineq[1]))
            pred_op = a_ineq[1] + " "
            if datatype == 'str':
                pred_op += f"LIKE {get_format(datatype, a_ineq[3])}"
            else:
                pred_op = handle_range_preds(datatype, a_ineq, pred_op)
            predicates.append(pred_op)
            self.filter_predicates.append(a_ineq)
        for aoa in self.aoa_predicates:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" <= ".join(pred))
        for aoa in self.aoa_less_thans:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" < ".join(pred))

        self.where_clause = "\n and ".join(predicates)

    def doActualJob(self, args):
        self.filter_extractor.mock = self.mock
        query = self.extract_params_from_args(args)
        self.init_constants()
        check = self.filter_extractor.doJob(query)
        if not check:
            return False
        self.filter_extractor.logger.debug("Filters: ", self.filter_extractor.filter_predicates)
        partition_eq_dict, ineqaoa_preds = self.make_equiJoin_groups()
        self.find_eq_join_graph(query, partition_eq_dict)

        for eq_join in self.algebraic_eq_predicates:
            for pred in eq_join:
                if len(pred) == 5:
                    ineqaoa_preds.append(pred)

        # self.aoa_predicates, maps = self.find_ineq_aoa_algo3(query, ineqaoa_preds)
        self.find_ineq_aoa_algo3_1(query, ineqaoa_preds)

        # optimize_edge_set(self.aoa_predicates)

        # self.revert_mutation_on_filter_global_min_instance_dict()
        # self.generate_where_clause()
        # self.post_process_for_generation_pipeline()
        # return True

        # directed_paths = find_all_chains(create_adjacency_map_from_aoa_predicates(self.aoa_predicates))
        # a_LBs, a_UBs, aoa_LBs, aoa_UBs = maps[0], maps[1], maps[2], maps[3]
        # print(self.aoa_predicates)
        # for path in directed_paths:
        # self.check_for_dormant_CB(path, a_LBs, aoa_LBs, query, False)
        #    self.check_for_dormant_CB(path, a_UBs, aoa_UBs, query, True)

        # self.organize_less_thans()
        # optimize_edge_set(self.aoa_predicates)
        self.revert_mutation_on_filter_global_min_instance_dict()

        self.generate_where_clause()
        self.post_process_for_generation_pipeline()
        return True

    def organize_less_thans(self):
        for aoa in self.aoa_less_thans:
            try:
                self.aoa_predicates.remove(aoa)
            except ValueError:
                self.logger.debug("Weired!")
                pass

    def add_CB_to_aoa(self, datatype: str, cb_b, is_UB: bool):
        if is_UB:
            l_val = get_val_plus_delta(datatype, get_UB(cb_b), 2 * get_delta(self.constants_dict[datatype]))
            if l_val == get_max(self.constants_dict[datatype]):
                return False
            if get_UB(cb_b) != get_max(self.constants_dict[datatype]):
                self.aoa_predicates.append([(get_tab(cb_b), get_attrib(cb_b)), get_UB(cb_b)])
        else:
            l_val = get_val_plus_delta(datatype, get_LB(cb_b), -2 * get_delta(self.constants_dict[datatype]))
            if l_val == get_min(self.constants_dict[datatype]):
                return False
            if get_LB(cb_b) != get_min(self.constants_dict[datatype]):
                self.aoa_predicates.append([get_LB(cb_b), (get_tab(cb_b), get_attrib(cb_b))])
        return True

    def get_equi_join_group(self, tab_attrib: tuple[str, str]) -> list[tuple[str, str]]:
        for eq in self.algebraic_eq_predicates:
            if tab_attrib in eq:
                return eq
        return [tab_attrib]

    def check_for_dormant_CB(self, directed_paths: list[tuple[str, str]], a_Bs, aoa_Bs, query: str, is_UB: bool):
        if not len(directed_paths):
            return []
        tab_attrib, pending_path = split_directed_path(directed_paths, is_UB)
        tab_attrib_eq_group = self.get_equi_join_group(tab_attrib)
        filter_attribs = []
        datatype = self.get_datatype(tab_attrib)
        self.mutate_with_boundary_value(a_Bs, aoa_Bs, datatype, filter_attribs, is_UB, query, tab_attrib,
                                        tab_attrib_eq_group)

        not_cb = set()
        for cb_b in filter_attribs:
            datatype = self.get_datatype((get_tab(cb_b), get_attrib(cb_b)))
            check = self.add_CB_to_aoa(datatype, cb_b, is_UB)
            if not check:
                not_cb.add(cb_b)
        for cb_b in not_cb:
            filter_attribs.remove(cb_b)

        remaining_path_filter_attribs = self.check_for_dormant_CB(pending_path, a_Bs, aoa_Bs, query, is_UB)
        filter_attribs.extend(remaining_path_filter_attribs)
        return filter_attribs

    def mutate_with_boundary_value(self, a_Bs, aoa_Bs, datatype, filter_attribs, is_UB, query, tab_attrib,
                                   tab_attrib_eq_group):
        val = None
        for key in tab_attrib_eq_group:
            if key in aoa_Bs.keys():
                val = aoa_Bs[key]
        if val is None:
            min_val, max_val = get_min_max_for_chain_bounds(get_min(self.constants_dict[datatype]),
                                                            get_max(self.constants_dict[datatype]),
                                                            tab_attrib, a_Bs, is_UB)
            prep = self.filter_extractor.prepare_attrib_set_for_bulk_mutation(tab_attrib_eq_group)
            self.filter_extractor.handle_filter_for_nonTextTypes(prep, datatype, filter_attribs, max_val, min_val,
                                                                 query)
            val = get_val_bound_for_chain(get_min(self.constants_dict[datatype]),
                                          get_max(self.constants_dict[datatype]),
                                          filter_attribs, is_UB)
        for key in tab_attrib_eq_group:
            tab, attrib = key[0], key[1]
            self.mutate_filter_global_min_instance_dict(tab, attrib, val)
            self.connectionHelper.execute_sql([update_tab_attrib_with_value(attrib, tab, get_format(datatype, val))])
            self.logger.debug(update_tab_attrib_with_value(attrib, tab, get_format(datatype, val)))

    def mutate_filter_global_min_instance_dict(self, tab: str, attrib: str, val):
        g_min_dict = self.filter_extractor.global_min_instance_dict
        data = g_min_dict[tab]
        idx = data[0].index(attrib)
        new_data = []
        for i in range(0, len(data[1])):
            if idx == i:
                new_data.append(val)
            else:
                new_data.append(data[1][i])
        data[1] = tuple(new_data)

    def revert_mutation_on_filter_global_min_instance_dict(self):
        self.filter_extractor.global_min_instance_dict = copy.deepcopy(self.global_min_instance_dict)

    def do_bound_check_again(self, tab_attrib: tuple[str, str], datatype: str, query: str) -> list:
        filter_attribs = []
        d_plus_value = copy.deepcopy(self.filter_extractor.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.filter_extractor.global_attrib_max_length)
        one_attrib = (tab_attrib[0], tab_attrib[1], attrib_max_length, d_plus_value)
        self.filter_extractor.extract_filter_on_attrib_set(filter_attribs, query, [one_attrib], datatype)
        return filter_attribs

    def is_dmin_val_leq_LB(self, myself, other) -> bool:
        val = self.get_dmin_val(get_attrib(myself), get_tab(myself))
        satisfied = self.do_numeric_drama(get_LB(other), get_UB(myself), get_attrib(myself), get_tab(myself), val)
        return satisfied

    def do_numeric_drama(self, other_LB, my_UB, attrib, tab, my_val):
        datatype = self.get_datatype((tab, attrib))
        satisfied = my_val <= other_LB  # <= _oB
        # all the following DRAMA is to handle "numeric" datatype
        if datatype == 'numeric':
            bck_diff_1 = my_val - other_LB
            # bck_diff_2 = _B - _oB
            alt_sat = True
            if not satisfied:
                if bck_diff_1 > 0:
                    alt_sat = alt_sat & (abs(bck_diff_1) <= get_delta(self.constants_dict[datatype]))
                # if bck_diff_2 > 0:
                #    alt_sat = alt_sat & (abs(bck_diff_2) <= delta)
            return alt_sat or satisfied
        return satisfied

    def find_ineq_aoa_algo3(self,
                            query: str,
                            ineqaoa_preds: list[tuple[str, str, str,
                            Union[int, date, float],
                            Union[int, date, float]]]) -> tuple[
        list[tuple[tuple[str, str], tuple[str, str]]], tuple[dict, dict, dict, dict]]:

        absorbed_UBs, absorbed_LBs, aoa_CB_UBs, aoa_CB_LBs = {}, {}, {}, {}
        filtered_dict = self.isolate_ineq_aoa_preds_per_datatype(ineqaoa_preds)
        E = []
        for key in filtered_dict:
            edge_set = []
            ineq_group = filtered_dict[key]
            C_E = create_attrib_set_from_filter_predicates(ineq_group)
            self.create_dashed_edges(ineq_group, edge_set)
            add_concrete_bounds_as_edge(ineq_group, edge_set)
            unvisited = copy.deepcopy(C_E)
            for attrib in unvisited:
                C_next = get_out_edges(edge_set, attrib)
                for c_attrib in C_next:
                    v_prev = get_LB_of_next_attrib(ineq_group, c_attrib)
                    UB_impact = self.is_nextLB_impacted_by_Bound(attrib, c_attrib, ineq_group, key, query, v_prev, True)
                    LB_impact = self.is_nextLB_impacted_by_Bound(attrib, c_attrib, ineq_group, key, query, v_prev,
                                                                 False)
                    adjust_Bounds2(LB_impact, UB_impact,
                                   absorbed_LBs, absorbed_UBs,
                                   aoa_CB_LBs, aoa_CB_UBs,
                                   attrib, c_attrib, edge_set)
            left_over_aoa_CBs(absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, edge_set)
            E.extend(edge_set)
        return E, (absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs)

    def find_ineq_aoa_algo3_1(self,
                              query: str,
                              ineqaoa_preds: list[tuple[str, str, str,
                              Union[int, date, float],
                              Union[int, date, float]]]) -> None:

        absorbed_UBs, absorbed_LBs, aoa_CB_UBs, aoa_CB_LBs = {}, {}, {}, {}
        filtered_dict = self.isolate_ineq_aoa_preds_per_datatype(ineqaoa_preds)
        for key in filtered_dict:
            edge_set = []
            ineq_group = filtered_dict[key]
            self.create_dashed_edges(ineq_group, edge_set)
            optimize_edge_set(edge_set)
            add_concrete_bounds_as_edge(ineq_group, edge_set)
            c_e_dict = create_adjacency_map_from_aoa_predicates(edge_set)
            directed_paths = find_all_chains(c_e_dict)
            for path in directed_paths:
                while len(path) > 1:
                    path = self.extract_ineq_for_an_edge(absorbed_LBs, absorbed_UBs,
                                                         aoa_CB_LBs, aoa_CB_UBs,
                                                         edge_set,
                                                         ineq_group,
                                                         key, path, query)

            self.aoa_predicates.extend(edge_set)

    def extract_ineq_for_an_edge(self, absorbed_LBs, absorbed_UBs,
                                 aoa_CB_LBs, aoa_CB_UBs,
                                 edge_set,
                                 ineq_group,
                                 datatype,
                                 path,
                                 query):
        tab_attrib, pending_path = split_directed_path(path, False)
        next_attrib = pending_path[0]
        prev_lb = get_LB_of_next_attrib(ineq_group, next_attrib)
        UB_impact = self.is_nextLB_impacted_by_Bound(tab_attrib, next_attrib, ineq_group, datatype, query, prev_lb,
                                                     True)
        LB_impact = self.is_nextLB_impacted_by_Bound(tab_attrib, next_attrib, ineq_group, datatype, query, prev_lb,
                                                     False)
        solid_edge = adjust_Bounds2(LB_impact, UB_impact,
                                    absorbed_LBs, absorbed_UBs,
                                    aoa_CB_LBs, aoa_CB_UBs,
                                    tab_attrib, next_attrib, edge_set)
        left_over_aoa_CBs(absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, edge_set)
        if solid_edge:
            tab_attrib_eq_group = self.get_equi_join_group(tab_attrib)
            filter_attribs = []
            self.mutate_with_boundary_value(absorbed_LBs, aoa_CB_LBs, datatype, filter_attribs, False, query,
                                            tab_attrib,
                                            tab_attrib_eq_group)
            self.update_ineq_group(datatype, ineq_group, [tab_attrib, next_attrib], query)
            not_cb = set()
            for cb_b in filter_attribs:
                check = self.add_CB_to_aoa(datatype, cb_b, False)  # or self.add_CB_to_aoa(key, cb_b, True)
                if not check:
                    self.aoa_less_thans.append((tab_attrib, next_attrib))
                    edge_set.remove((tab_attrib, next_attrib))
                    not_cb.add(cb_b)
            for cb_b in not_cb:
                filter_attribs.remove(cb_b)
        path = pending_path
        return path

    def update_ineq_group(self, datatype: str,
                          ineq_group: list,
                          tab_attrib_list: list[tuple[str, str]],
                          query: str):
        to_remove = []
        to_add = []
        for tab_attrib in tab_attrib_list:
            f_e = self.do_bound_check_again(tab_attrib, datatype, query)
            if not f_e:
                to_add.append((get_tab(tab_attrib), get_attrib(tab_attrib), 'range',
                               get_min(self.constants_dict[datatype]),
                               get_max(self.constants_dict[datatype])))
            else:
                to_add.append(f_e[0])
            for i in range(len(ineq_group)):
                pred = ineq_group[i]
                if get_tab(pred) == get_tab(tab_attrib) and get_attrib(pred) == get_attrib(tab_attrib):
                    to_remove.append(i)
        to_remove.sort(reverse=True)
        for idx in to_remove:
            if ineq_group[idx][2] == '=' or ineq_group[idx][2] == 'equal':
                try:
                    self.arithmetic_eq_predicates.remove(ineq_group[idx])
                except ValueError:
                    pass
            del ineq_group[idx]
        ineq_group.extend(to_add)

    def is_nextLB_impacted_by_Bound(self, attrib: tuple[str, str], next_attrib: tuple[str, str],
                                    ineq_group: list[tuple[str, str, str, any, any]],
                                    datatype: str,
                                    query: str,
                                    prev_lb: Union[int, float, date],
                                    is_UB: bool) -> bool:
        old_dmin_dict = copy.deepcopy(self.global_min_instance_dict)
        self.mutate_attrib_with_Bound_val(attrib, datatype, ineq_group, is_UB)
        next_lb = self.do_bound_check_again(next_attrib, datatype, query)
        self.global_min_instance_dict = old_dmin_dict
        _impact = False
        if len(next_lb) > 0 and prev_lb != get_LB(next_lb[0]):
            _impact = True
        elif not next_lb \
                and prev_lb != get_min(self.constants_dict[datatype]) \
                and prev_lb != get_max(self.constants_dict[datatype]):
            _impact = True
        return _impact

    def create_dashed_edges(self, ineq_group, edge_set):
        seq = get_all_two_combs(ineq_group)
        for e in seq:
            one, two = e[0], e[1]
            self.create_dashed_edge_from_oneTotwo(edge_set, one, two)
            self.create_dashed_edge_from_oneTotwo(edge_set, two, one)

    def create_dashed_edge_from_oneTotwo(self, edge_set, one, two):
        tab_attrib = (get_tab(one), get_attrib(one))
        next_tab_attrib = (get_tab(two), get_attrib(two))
        check = self.is_dmin_val_leq_LB(two, one)
        if check:
            edge_set.append(tuple([next_tab_attrib, tab_attrib]))

    def isolate_ineq_aoa_preds_per_datatype(self,
                                            ineqaoa_preds: list[tuple[str, str, str,
                                            Union[int, date, float],
                                            Union[int, date, float]]]) -> dict:
        datatype_dict = {}
        ineqaoa_preds.extend(self.arithmetic_eq_predicates)
        for pred in ineqaoa_preds:
            tab_attrib = (pred[0], pred[1])
            datatype = self.get_datatype(tab_attrib)
            if datatype in datatype_dict.keys():
                datatype_dict[datatype].append(pred)
            else:
                datatype_dict[datatype] = [pred]
        filtered_dict = {key: value for key, value in datatype_dict.items() if key != 'str' and len(value) > 1}
        for key in datatype_dict:
            if len(datatype_dict[key]) == 1:
                self.arithmetic_ineq_predicates.extend(datatype_dict[key])
        return filtered_dict

    def find_eq_join_graph(self, query: str, partition_eq_dict: dict):
        # self.logger.debug(partition_eq_dict)
        while partition_eq_dict:
            check_again_dict = {}
            for key in partition_eq_dict.keys():
                equi_join_group = partition_eq_dict[key]
                if len(equi_join_group) <= 3:
                    self.handle_unit_eq_group(equi_join_group, query)
                else:
                    done = self.handle_higher_eq_groups(equi_join_group, query)
                    remaining_group = [eq for eq in equi_join_group if eq not in done]
                    check_again_dict[key] = remaining_group
            partition_eq_dict = check_again_dict
        # self.logger.debug(self.algebraic_eq_predicates)

    def handle_unit_eq_group(self, equi_join_group, query):
        filter_attribs = []
        datatype = self.get_datatype(equi_join_group[0])
        prepared_attrib_list = self.filter_extractor.prepare_attrib_set_for_bulk_mutation(equi_join_group)
        self.filter_extractor.extract_filter_on_attrib_set(filter_attribs, query, prepared_attrib_list,
                                                           datatype)
        if len(filter_attribs) > 0:
            if filter_attribs[0][2] == '=' or filter_attribs[0][2] == 'equal':
                return False
            equi_join_group.extend(filter_attribs)
        self.algebraic_eq_predicates.append(equi_join_group)
        return True

    def make_equiJoin_groups(self):
        eq_groups_dict = {}
        ineq_filter_predicates = []
        for pred in self.filter_extractor.filter_predicates:
            if pred[2] == '=' or pred[2] == 'equal':
                dict_key = pred[3]
                if dict_key in eq_groups_dict:
                    eq_groups_dict[dict_key].append((pred[0], pred[1]))
                else:
                    eq_groups_dict[dict_key] = [(pred[0], pred[1])]
            else:
                ineq_filter_predicates.append(pred)

        for key in eq_groups_dict.keys():
            if len(eq_groups_dict[key]) == 1:
                if isinstance(key, str):
                    op = 'equal'
                else:
                    op = '='
                self.arithmetic_eq_predicates.append((get_tab(eq_groups_dict[key][0]),
                                                      get_attrib(eq_groups_dict[key][0]), op, key, key))
        eqJoin_group_dict = {key: value for key, value in eq_groups_dict.items() if len(value) > 1}
        return eqJoin_group_dict, ineq_filter_predicates

    def handle_higher_eq_groups(self, equi_join_group, query):
        seq = list(range(len(equi_join_group)))
        t_all_paritions = merge_equivalent_paritions(seq)
        done = None
        for part in t_all_paritions:
            check_part = min(part, key=len)
            attrib_list = []
            for i in check_part:
                attrib_list.append(equi_join_group[i])
            check = self.handle_unit_eq_group(attrib_list, query)
            if check:
                done = attrib_list
                break
        return done

    def mutate_attrib_with_Bound_val(self, tab_attrib: tuple[str, str],
                                     datatype: str,
                                     ineq_group: list, with_UB: bool) -> None:
        for pred in ineq_group:
            if get_tab(tab_attrib) == get_tab(pred) and get_attrib(tab_attrib) == get_attrib(pred):
                dmin_val = self.get_dmin_val(get_attrib(tab_attrib), get_tab(tab_attrib))
                if with_UB:
                    bound = get_UB(pred)
                    if dmin_val == bound:
                        bound = get_val_plus_delta(datatype, bound, -1 * get_delta(self.constants_dict[datatype]))
                else:
                    bound = get_LB(pred)
                    if dmin_val == bound:
                        bound = get_val_plus_delta(datatype, bound, get_delta(self.constants_dict[datatype]))
                # print(f"attrib {get_attrib(tab_attrib)} mutate with {bound}")
                self.connectionHelper.execute_sql([update_tab_attrib_with_value(get_attrib(tab_attrib),
                                                                                get_tab(tab_attrib),
                                                                                get_format(datatype, bound))])
                self.mutate_filter_global_min_instance_dict(get_tab(tab_attrib),
                                                            get_attrib(tab_attrib), bound)
                break
