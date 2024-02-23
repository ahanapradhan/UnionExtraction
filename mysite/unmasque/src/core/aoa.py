import copy
from datetime import date
from typing import Union

from mysite.unmasque.refactored.abstract.MutationPipeLineBase import MutationPipeLineBase
from mysite.unmasque.refactored.filter import Filter, get_constants_for
from mysite.unmasque.refactored.util.common_queries import update_tab_attrib_with_value
from mysite.unmasque.refactored.util.utils import get_format, get_datatype_of_val, get_min_and_max_val, \
    get_val_plus_delta
from mysite.unmasque.src.core.QueryStringGenerator import handle_range_preds
from mysite.unmasque.src.core.dataclass.generation_pipeline_package import PackageForGenPipeline
from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper


def is_sublist(lst, sublist):
    lst_hash = [hash(el) for el in lst]
    sublist_hash = [hash(el) for el in sublist]

    seq = []
    for s in sublist_hash:
        if s in lst_hash:
            s_idx = lst_hash.index(s)
            seq.append(s_idx)
    if not len(seq):
        return False
    start = seq[0]
    for i in range(1, len(seq)):
        if start + 1 != seq[i]:
            return False
        start = start + 1
    return True


def remove_subchians(chains):
    to_remove = []
    seq = get_all_two_combs(chains)
    for s in seq:
        if is_sublist(s[0], s[1]):
            to_remove.append(s[1])
    for r in to_remove:
        chains.remove(r)
    return chains


def find_chains(graph, start, path=[]):
    path = path + [start]
    if start not in graph:
        return [path]
    chains = []
    for node in graph[start]:
        if node not in path:
            new_chains = find_chains(graph, node, path)
            for new_chain in new_chains:
                chains.append(new_chain)
    return chains


def find_all_chains(input_map):
    # Find all possible chains
    all_chains = []
    for key in input_map:
        chains = find_chains(input_map, key)
        all_chains.extend(chains)
    _all_chains = remove_subchians(all_chains)
    return _all_chains


def get_all_two_combs(items):
    seq = []
    for i in range(0, len(items)):
        for j in range(i + 1, len(items)):
            seq.append([items[i], items[j]])
    return seq


def get_LB(pred):
    return pred[3]


def get_UB(pred):
    return pred[4]


def get_attrib(pred):
    return pred[1]


def get_tab(pred):
    return pred[0]


def create_attrib_set_from_filter_predicates(ineq_group):
    C_E = set()
    for pred in ineq_group:
        C_E.add((get_tab(pred), get_attrib(pred)))
    return list(C_E)


def create_adjacency_map_from_aoa_predicates(aoa_preds, skip_tuples=True):
    C_E = {}
    for pred in aoa_preds:
        if skip_tuples and (not isinstance(pred[0], tuple) or not isinstance(pred[1], tuple)):
            continue
        if pred[0] in C_E.keys():
            C_E[pred[0]].append(pred[1])
        else:
            C_E[pred[0]] = [pred[1]]
    return C_E


def get_out_edges(edge_set, tab_attrib):
    next_attribs = []
    for edge in edge_set:
        if edge[0] == tab_attrib and isinstance(edge[1], tuple) and len(edge[1]) == 2:
            next_attribs.append(edge[1])
    return next_attribs


def get_concrete_LB_in_edge(edge_set, tab_attrib):
    for edge in edge_set:
        if edge[1] == tab_attrib and not isinstance(edge[0], tuple):
            return edge


def get_concrete_UB_out_edge(edge_set, tab_attrib):
    for edge in edge_set:
        if edge[0] == tab_attrib and not isinstance(edge[1], tuple):
            return edge


def left_over_aoa_CBs(absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, edge_set):
    for edge in edge_set:
        if not isinstance(edge[0], tuple):
            if edge[1] not in aoa_CB_LBs or edge[1] not in absorbed_LBs:
                aoa_CB_LBs[edge[1]] = edge[0]
        if not isinstance(edge[1], tuple):
            if edge[0] not in aoa_CB_UBs or edge[0] not in absorbed_UBs:
                aoa_CB_UBs[edge[0]] = edge[1]


def partitions_with_min_elements(arr, min_elements=2):
    n = len(arr)

    # Memoization dictionary to store already computed partitions
    memo = {}

    def dp(index, subsets, min_elements):
        if index == n:
            key = tuple(tuple(sorted(subset)) for subset in subsets)
            if all(len(subset) >= min_elements for subset in subsets):
                return [subsets] if key not in memo else []
            else:
                return []

        key = (index, tuple(tuple(sorted(subset)) for subset in subsets))
        if key in memo:
            return memo[key]

        result = []

        for i, subset in enumerate(subsets):
            new_subsets = subsets[:i] + [subset + [arr[index]]] + subsets[i + 1:]
            result += dp(index + 1, new_subsets, min_elements)

        if len(subsets) < min_elements:
            result += dp(index + 1, subsets + [[arr[index]]], min_elements)

        memo[key] = result
        return result

    partitions = dp(0, [[]] * min_elements, min_elements)
    return partitions


def process_entry(l):
    _l = []
    for em in l:
        em_set = frozenset(em)
        _l.append(em_set)
    return frozenset(_l)


def merge_equivalent_paritions(arr):
    paritions = partitions_with_min_elements(arr)
    em_dict = {}
    for p in paritions:
        s = process_entry(p)
        key = hash(s)
        if key not in em_dict:
            em_dict[key] = s
    my_list = list(em_dict.values())
    return my_list


def add_concrete_bounds_as_edge(pred_list, edge_set):
    for pred in pred_list:
        if pred[2] == '<=':
            edge_set.append([(get_tab(pred), get_attrib(pred)), get_UB(pred)])
        elif pred[2] == '>=':
            edge_set.append([get_LB(pred), (get_tab(pred), get_attrib(pred))])
        elif pred[2] == 'range':
            edge_set.append([get_LB(pred), (get_tab(pred), get_attrib(pred))])
            edge_set.append([(get_tab(pred), get_attrib(pred)), get_UB(pred)])


def create_v_lj_2(ineq_group, c):
    tab, attrib = c[0], c[1]
    for pred in ineq_group:
        if pred[0] == tab and pred[1] == attrib and (pred[2] == '>='):
            return pred[3]


def add_pred_for(aoa_l, pred):
    if isinstance(aoa_l, list) or isinstance(aoa_l, tuple):
        pred.append(aoa_l[1])
    else:
        pred.append(get_format(get_datatype_of_val(aoa_l), aoa_l))
    return aoa_l


def adjust_Bounds(LB_impact, UB_impact, absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, attrib, c, edge_set):
    _LB = get_concrete_LB_in_edge(edge_set, c)
    _UB = get_concrete_UB_out_edge(edge_set, attrib)
    absorbed = False
    if UB_impact and LB_impact:
        if _LB:
            absorbed_LBs[_LB[1]] = _LB[0]
            edge_set.remove(_LB)
        if _UB:
            absorbed_UBs[_UB[0]] = _UB[1]
            edge_set.remove(_UB)
        absorbed = True
    elif not UB_impact and not LB_impact:
        edge_set.remove((attrib, c))
    if not absorbed:
        if _LB:
            aoa_CB_LBs[_LB[1]] = _LB[0]
        if _UB:
            aoa_CB_UBs[_UB[0]] = _UB[1]


def adjust_Bounds2(LB_impact, UB_impact, absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, attrib, c, edge_set):
    _LB = get_concrete_LB_in_edge(edge_set, c)
    _UB = get_concrete_UB_out_edge(edge_set, attrib)
    if _UB:
        if UB_impact:
            absorbed_UBs[_UB[0]] = _UB[1]
            edge_set.remove(_UB)
        else:
            aoa_CB_UBs[_UB[0]] = _UB[1]
    if _LB:
        if LB_impact:
            absorbed_LBs[_LB[1]] = _LB[0]
            edge_set.remove(_LB)
        else:
            aoa_CB_LBs[_LB[1]] = _LB[0]

    if not UB_impact and not LB_impact:
        edge_set.remove((attrib, c))


def get_val_bound_for_chain(i_min, i_max, filter_attrib, is_UB):
    if not len(filter_attrib):
        if is_UB:
            val = i_max
        else:
            val = i_min
    else:
        if is_UB:
            val = filter_attrib[0][4]
        else:
            val = filter_attrib[0][3]
    return val


def get_min_max_for_chain_bounds(i_min, i_max, tab_attrib, a_b, is_UB):
    if is_UB:
        if tab_attrib in a_b.keys():
            min_val = a_b[tab_attrib]
        else:
            min_val = i_min
        max_val = i_max
    else:
        if tab_attrib in a_b.keys():
            max_val = a_b[tab_attrib]
        else:
            max_val = i_max
        min_val = i_min
    return min_val, max_val


def split_directed_path(directed_path, is_UB):
    if is_UB:
        return directed_path[-1], directed_path[:-1]
    else:
        return directed_path[0], directed_path[1:]


def optimize_by_matrix(C_E, aoa_predicates, h, w):
    m = [[0 for x in range(w)] for y in range(h)]
    for pred in aoa_predicates:
        src_idx = C_E.index(pred[0])
        snk_idx = C_E.index(pred[1])
        m[src_idx][snk_idx] = 1
    to_remove = []
    for x in range(h):
        for y in range(h):
            if x != y:
                for c in range(w):
                    if m[x][c] and m[y][c] and m[x][y]:
                        to_remove.append((x, c))
    for r in to_remove:
        x, c = r[0], r[1]
        m[x][c] = 0
    return m


class AlgebraicPredicate(MutationPipeLineBase):
    def __init__(self, connectionHelper: ConnectionHelper, core_relations: list[str], global_min_instance_dict: dict):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, "AlgebraicPredicate")
        self.filter_extractor = Filter(self.connectionHelper, core_relations, global_min_instance_dict)

        self.get_datatype = self.filter_extractor.get_datatype  # method

        self.pipeline_delivery = None

        self.aoa_predicates = None
        self.arithmetic_eq_predicates = []
        self.algebraic_eq_predicates = []
        self.arithmetic_ineq_predicates = []
        self.aoa_less_thans = []
        self.global_min_instance_dict_bkp = copy.deepcopy(global_min_instance_dict)

        self.where_clause = ""

        self.join_graph = []
        self.filter_predicates = []

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

        self.aoa_predicates, maps = self.find_ineq_aoa_algo3(query, ineqaoa_preds)
        self.optimize_aoa()

        directed_paths = find_all_chains(create_adjacency_map_from_aoa_predicates(self.aoa_predicates))
        a_LBs, a_UBs, aoa_LBs, aoa_UBs = maps[0], maps[1], maps[2], maps[3]
        print(self.aoa_predicates)
        for path in directed_paths:
            self.check_for_dormant_CB(path, a_LBs, aoa_LBs, query, False)
            self.check_for_dormant_CB(path, a_UBs, aoa_UBs, query, True)

        self.organize_less_thans()
        self.optimize_aoa()
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

    def optimize_aoa(self):
        nodes = set()
        for pred in self.aoa_predicates:
            nodes.add(pred[0])
            nodes.add(pred[1])
        C_E = list(nodes)
        w, h = len(C_E), len(C_E)
        m = optimize_by_matrix(C_E, self.aoa_predicates, h, w)
        for x in range(h):
            for c in range(w):
                if not m[x][c]:
                    for aoa in self.aoa_predicates:
                        if aoa[0] == C_E[x] and aoa[1] == C_E[c]:
                            self.aoa_predicates.remove(aoa)

    def add_CB_to_aoa(self, datatype: str, cb_b, is_UB: bool):
        i_min, i_max = get_min_and_max_val(datatype)
        delta, while_cut_off = get_constants_for(datatype)
        if is_UB:
            l_val = get_val_plus_delta(datatype, get_UB(cb_b), 2 * delta)
            if l_val == i_max:
                return False
            if get_UB(cb_b) != i_max:
                self.aoa_predicates.append([(get_tab(cb_b), get_attrib(cb_b)), get_UB(cb_b)])
        else:
            l_val = get_val_plus_delta(datatype, get_LB(cb_b), -2 * delta)
            if l_val == i_min:
                return False
            if get_LB(cb_b) != i_min:
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
                # it is less then relationship. Not less than equl to. How to represent it?
                if len(pending_path) and not is_UB:
                    other_tab_attrib = pending_path[0]
                    self.logger.debug(f"{cb_b[1]} < {other_tab_attrib[1]}. Not <=. How to indicate it? ", cb_b)
                    self.aoa_less_thans.append((tab_attrib, other_tab_attrib))
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
            i_min, i_max = get_min_and_max_val(datatype)
            min_val, max_val = get_min_max_for_chain_bounds(i_min, i_max, tab_attrib, a_Bs, is_UB)
            prep = self.filter_extractor.prepare_attrib_set_for_bulk_mutation(tab_attrib_eq_group)
            self.filter_extractor.handle_filter_for_nonTextTypes(prep, datatype, filter_attribs, max_val, min_val,
                                                                 query)
            val = get_val_bound_for_chain(i_min, i_max, filter_attribs, is_UB)
        for key in tab_attrib_eq_group:
            tab, attrib = key[0], key[1]
            self.mutate_filter_global_min_instance_dict(tab, attrib, val)
            self.connectionHelper.execute_sql([update_tab_attrib_with_value(attrib, tab, get_format(datatype, val))])

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

    def do_bound_check_again(self, tab_attrib: tuple[str, str], datatype: str, query: str):
        filter_attribs = []
        d_plus_value = copy.deepcopy(self.filter_extractor.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.filter_extractor.global_attrib_max_length)
        one_attrib = (tab_attrib[0], tab_attrib[1], attrib_max_length, d_plus_value)
        self.filter_extractor.extract_filter_on_attrib_set(filter_attribs, query, [one_attrib], datatype)
        return filter_attribs

    def is_dmin_val_leq_LB(self, other, myself):
        tab, attrib, _oB = myself[0], myself[1], myself[4]
        _B = other[3]
        val = self.get_dmin_val(attrib, tab)
        self.logger.debug(f"dmin.{attrib} = {val}, {other[1]}.LB = {_B}")
        satisfied = self.do_numeric_drama(_B, _oB, attrib, tab, val)
        return satisfied

    def do_numeric_drama(self, _B, _oB, attrib, tab, val):
        datatype = self.get_datatype((tab, attrib))
        satisfied = val <= _B  # <= _oB
        # all the following DRAMA is to handle "numeric" datatype
        if datatype == 'numeric':
            delta, _ = get_constants_for(datatype)
            bck_diff_1 = val - _B
            # bck_diff_2 = _B - _oB
            alt_sat = True
            if not satisfied:
                if bck_diff_1 > 0:
                    alt_sat = alt_sat & (abs(bck_diff_1) <= delta)
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
                    v_prev = create_v_lj_2(ineq_group, c_attrib)
                    UB_impact = self.is_impacted_by_Bound(attrib, c_attrib, ineq_group, key, query, v_prev, True)
                    LB_impact = self.is_impacted_by_Bound(attrib, c_attrib, ineq_group, key, query, v_prev, False)
                    adjust_Bounds2(LB_impact, UB_impact,
                                   absorbed_LBs, absorbed_UBs,
                                   aoa_CB_LBs, aoa_CB_UBs,
                                   attrib, c_attrib, edge_set)
            left_over_aoa_CBs(absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, edge_set)
            E.extend(edge_set)
        return E, (absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs)

    def is_impacted_by_Bound(self, attrib, c, ineq_group, key, query, v_prev, is_UB):
        datatype = self.get_datatype(attrib)
        i_min, i_max = get_min_and_max_val(datatype)
        self.mutate_attrib_with_Bound_val(attrib, key, ineq_group, is_UB)
        f_e = self.do_bound_check_again(c, key, query)
        self.revert_mutation_on_filter_global_min_instance_dict()
        _impact = False
        # print(f"attrib {c} v_prev {v_prev} v_new {f_e[0][3]}")
        if len(f_e) > 0 and v_prev != f_e[0][3]:
            _impact = True
        elif not f_e and v_prev != i_min and v_prev != i_max:
            _impact = True
        return _impact

    def create_dashed_edges(self, ineq_group, edge_set):
        seq = get_all_two_combs(ineq_group)
        for e in seq:
            one, two = e[0], e[1]
            self.create_dashed_edge_from_oneTotwo(edge_set, one, two)
            self.create_dashed_edge_from_oneTotwo(edge_set, two, one)

    def create_dashed_edge_from_oneTotwo(self, edge_set, one, two):
        tab_attrib = (one[0], one[1])
        peer_tab_attrib = (two[0], two[1])
        check = self.is_dmin_val_leq_LB(one, two)
        if check:
            edge_set.append(tuple([peer_tab_attrib, tab_attrib]))

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
        filtered_dict = {key: value for key, value in datatype_dict.items() if len(value) > 1}
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

    def mutate_attrib_with_Bound_val(self, tab_attrib: tuple, datatype: str, ineq_group: list, with_UB: bool):
        for pred in ineq_group:
            if get_tab(tab_attrib) == get_tab(pred) and get_attrib(tab_attrib) == get_attrib(pred):
                dmin_val = self.get_dmin_val(get_attrib(tab_attrib), get_tab(tab_attrib))
                delta, _ = get_constants_for(datatype)
                if with_UB:
                    bound = get_UB(pred)
                    if dmin_val == bound:
                        bound = get_val_plus_delta(datatype, bound, -1 * delta)
                else:
                    bound = get_LB(pred)
                    if dmin_val == bound:
                        bound = get_val_plus_delta(datatype, bound, delta)
                # print(f"attrib {get_attrib(tab_attrib)} mutate with {bound}")
                self.connectionHelper.execute_sql([update_tab_attrib_with_value(get_attrib(tab_attrib),
                                                                                get_tab(tab_attrib),
                                                                                get_format(datatype, bound))])
                self.mutate_filter_global_min_instance_dict(get_tab(tab_attrib),
                                                            get_attrib(tab_attrib), bound)
                break

    def remove_transitive_aoa_chains(self):
        seqs = get_all_two_combs(self.aoa_predicates)
        to_remove = []
        for seq in seqs:
            # if isinstance(seq[0], tuple) and isinstance(seq[1], tuple):
            if seq[0][1] == seq[1][0] and (seq[0][0], seq[1][1]) in self.aoa_predicates:
                to_remove.append((seq[0][0], seq[1][1]))
        for t_r in to_remove:
            self.aoa_predicates.remove(t_r)
