import copy

from mysite.unmasque.refactored.abstract.where_clause import WhereClause
from mysite.unmasque.refactored.filter import Filter, get_constants_for
from mysite.unmasque.refactored.util.common_queries import update_tab_attrib_with_value
from mysite.unmasque.refactored.util.utils import get_format, get_datatype_of_val, get_mid_val, get_min_and_max_val
from mysite.unmasque.src.core.QueryStringGenerator import handle_range_preds


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


def remove_transitive_relations(input_list):
    result = []

    def is_transitive(tuple1, tuple2):
        return tuple1[1] == tuple2[0]

    for i in range(len(input_list)):
        for j in range(len(input_list)):
            if i != j and isinstance(input_list[i], tuple) and isinstance(input_list[j], tuple) \
                    and is_transitive(input_list[i], input_list[j]):
                break
        else:
            result.append(input_list[i])
    return result


def get_all_two_combs(items):
    seq = []
    for i in range(0, len(items)):
        for j in range(i + 1, len(items)):
            # print(f"{str(i)} {str(j)}")
            seq.append([items[i], items[j]])
    return seq


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
            edge_set.append([(pred[0], pred[1]), pred[4]])
        elif pred[2] == '>=':
            edge_set.append([pred[3], (pred[0], pred[1])])
        elif pred[2] == 'range':
            edge_set.append([pred[3], (pred[0], pred[1])])
            edge_set.append([(pred[0], pred[1]), pred[4]])


def create_attrib_set_from_filter_predicates(ineq_group):
    C_E = set()
    for pred in ineq_group:
        C_E.add((pred[0], pred[1]))
    return list(C_E)


def create_adjacency_map_from_aoa_predicates(aoa_preds):
    C_E = {}
    for pred in aoa_preds:
        if not isinstance(pred[0], tuple) or not isinstance(pred[1], tuple):
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


def create_v_lj(ineq_group, C_next):
    v_lj_list = []
    for tab_attrib in C_next:
        tab, attrib = tab_attrib[0], tab_attrib[1]
        for pred in ineq_group:
            if pred[0] == tab and pred[1] == attrib and pred[2] == '>=':
                v_lj_list.append(pred[3])
    return v_lj_list


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
        aoa_CB_LBs[_LB[1]] = _LB[0]
        aoa_CB_UBs[_UB[0]] = _UB[1]


def left_over_aoa_CBs(absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, edge_set):
    for edge in edge_set:
        if not isinstance(edge[0], tuple):
            if edge[1] not in aoa_CB_LBs or edge[1] not in absorbed_LBs:
                aoa_CB_LBs[edge[1]] = edge[0]
        if not isinstance(edge[1], tuple):
            if edge[0] not in aoa_CB_UBs or edge[0] not in absorbed_UBs:
                aoa_CB_UBs[edge[0]] = edge[1]


class AlgebraicPredicate(WhereClause):
    def __init__(self, connectionHelper, global_key_lists,
                 core_relations, global_min_instance_dict):
        super().__init__(connectionHelper, global_key_lists, core_relations, global_min_instance_dict,
                         "AlgebraicPredicate")
        self.filter_extractor = Filter(self.connectionHelper, global_key_lists,
                                       core_relations, global_min_instance_dict)
        self.aoa_predicates = None
        self.arithmetic_eq_predicates = None
        self.algebraic_eq_predicates = None
        self.arithmetic_ineq_predicates = None
        self.where_clause = ""

    def generate_where_clause(self):
        predicates = []
        for eq_join in self.algebraic_eq_predicates:
            join_edge = list(item[1] for item in eq_join)
            join_edge.sort()
            join_e = f"{join_edge[0]} = {join_edge[1]}"
            predicates.append(join_e)
        for a_eq in self.arithmetic_eq_predicates:
            datatype = self.filter_extractor.get_datatype((a_eq[0], a_eq[1]))
            pred = f"{a_eq[1]} = {get_format(datatype, a_eq[3])}"
            predicates.append(pred)
        for a_ineq in self.arithmetic_ineq_predicates:
            datatype = self.filter_extractor.get_datatype((a_ineq[0], a_ineq[1]))
            pred_op = a_ineq[1] + " "
            pred_op = handle_range_preds(datatype, a_ineq, pred_op)
            predicates.append(pred_op)
        for aoa in self.aoa_predicates:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" <= ".join(pred))

        self.where_clause = "\n and ".join(predicates)

    def doActualJob(self, args):
        self.filter_extractor.mock = self.mock
        query = self.extract_params_from_args(args)
        check = self.filter_extractor.doJob(query)
        if not check:
            return False
        partition_eq_dict, ineqaoa_preds = self.preprocess_for_aeqa()
        self.find_eq_join_graph(query, partition_eq_dict)

        for eq_join in self.algebraic_eq_predicates:
            if len(eq_join[-1]) == 5:
                ineqaoa_preds.append(eq_join[-1])

        self.aoa_predicates, maps = self.find_ineq_aoa_algo3(query, ineqaoa_preds)
        self.optimize_aoa_predicates()
        self.logger.debug("E: ", self.aoa_predicates)

        a_LBs, a_UBs, aoa_LBs, aoa_UBs = maps[0], maps[1], maps[2], maps[3]

        directed_paths = find_all_chains(create_adjacency_map_from_aoa_predicates(self.aoa_predicates))
        self.logger.debug(directed_paths)
        for path in directed_paths:
            self.find_cb_lbs(a_LBs, aoa_LBs, path, query)
            self.find_cb_ubs(a_UBs, aoa_UBs, path, query)
        self.revert_mutation_on_filter_global_min_instance_dict()
        self.generate_where_clause()
        return True

    def find_cb_lbs(self, a_LBs, aoa_LBs, path, query):
        cb_LBs = self.check_for_CB_LB(path, a_LBs, aoa_LBs, query)
        for cb_lb in cb_LBs:
            datatype = self.filter_extractor.get_datatype((cb_lb[0], cb_lb[1]))
            i_min, _ = get_min_and_max_val(datatype)
            if cb_lb[3] != i_min:
                self.aoa_predicates.append([cb_lb[3], (cb_lb[0], cb_lb[1])])
        self.logger.debug("cb_LBs:", cb_LBs)

    def check_for_CB_LB(self, directed_paths, a_LBs, aoa_LBs, query):
        if not len(directed_paths):
            return []
        tab_attrib = directed_paths[0]
        tab, attrib = tab_attrib[0], tab_attrib[1]
        filter_attribs = []
        datatype = self.filter_extractor.get_datatype(tab_attrib)
        if tab_attrib in aoa_LBs.keys():
            val = aoa_LBs[tab_attrib]
        else:
            i_min, i_max = get_min_and_max_val(datatype)

            if tab_attrib in a_LBs.keys():
                max_val = a_LBs[tab_attrib]
            else:
                max_val = i_max
            prep = self.filter_extractor.prepare_attrib_set_for_bulk_mutation([tab_attrib])
            self.filter_extractor.handle_filter_for_nonTextTypes(prep, datatype, filter_attribs, max_val, i_min, query)
            self.logger.debug(filter_attribs)
            if not len(filter_attribs):
                val = i_min
            else:
                val = filter_attribs[0][3]
        self.mutate_filter_global_min_instance_dict(tab, attrib, val)
        self.connectionHelper.execute_sql([update_tab_attrib_with_value(attrib, tab, get_format(datatype, val))])
        filter_attribs.extend(self.check_for_CB_LB(directed_paths[1:], a_LBs, aoa_LBs, query))
        return filter_attribs

    def mutate_filter_global_min_instance_dict(self, tab, attrib, val):
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
        self.filter_extractor.global_min_instance_dict = self.global_min_instance_dict

    def check_for_CB_UB(self, directed_paths, a_UBs, aoa_UBs, query):
        if not len(directed_paths):
            return []
        tab_attrib = directed_paths[-1]
        tab, attrib = tab_attrib[0], tab_attrib[1]
        filter_attribs = []
        datatype = self.filter_extractor.get_datatype(tab_attrib)
        if tab_attrib in aoa_UBs.keys():
            val = aoa_UBs[tab_attrib]
        else:
            i_min, i_max = get_min_and_max_val(datatype)

            if tab_attrib in a_UBs.keys():
                min_val = a_UBs[tab_attrib]
            else:
                min_val = i_max
            prep = self.filter_extractor.prepare_attrib_set_for_bulk_mutation([tab_attrib])
            self.filter_extractor.handle_filter_for_nonTextTypes(prep, datatype, filter_attribs, i_max, min_val, query)
            self.logger.debug(filter_attribs)
            if not len(filter_attribs):
                val = i_max
            else:
                val = filter_attribs[0][4]
        self.mutate_filter_global_min_instance_dict(tab, attrib, val)
        self.connectionHelper.execute_sql([update_tab_attrib_with_value(attrib, tab, get_format(datatype, val))])
        filter_attribs.extend(self.check_for_CB_LB(directed_paths[1:], a_UBs, aoa_UBs, query))
        return filter_attribs

    def do_bound_check_again(self, tab_attrib, datatype, query):
        filter_attribs = []
        d_plus_value = copy.deepcopy(self.filter_extractor.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.filter_extractor.global_attrib_max_length)
        one_attrib = (tab_attrib[0], tab_attrib[1], attrib_max_length, d_plus_value)
        self.filter_extractor.extract_filter_on_attrib_set(filter_attribs, query, [one_attrib], datatype)
        # self.logger.debug("filter_attribs", filter_attribs)
        return filter_attribs

    def is_dmin_val_same_as_B(self, pred, is_UB, datatype, peer_tab_attrib):
        tab, attrib = pred[0], pred[1]
        peer_tab, peer_attrib = peer_tab_attrib[0], peer_tab_attrib[1]
        _B = pred[4] if is_UB else pred[3]
        values = self.global_min_instance_dict[peer_tab]
        attribs, vals = values[0], values[1]
        attrib_idx = attribs.index(peer_attrib)
        val = vals[attrib_idx]
        # self.logger.debug(f"{tab}.{attrib} vs. {peer_tab}.{peer_attrib}: {str(val)}, {str(_B)}")

        if datatype == 'numeric':
            delta, _ = get_constants_for(datatype)
            result = max(_B, val) - min(_B, val)
            return result <= delta
        return val == _B

    def is_dmin_val_lessEqualTo_as_B(self, other, myself):
        tab, attrib, _oB = myself[0], myself[1], myself[4]
        _B = other[3]
        values = self.global_min_instance_dict[tab]
        attribs, vals = values[0], values[1]
        attrib_idx = attribs.index(attrib)
        val = vals[attrib_idx]
        return val <= _B <= _oB

    def find_ineq_aoa_algo3(self, query, ineqaoa_preds):
        absorbed_UBs, absorbed_LBs, aoa_CB_UBs, aoa_CB_LBs = {}, {}, {}, {}
        filtered_dict = self.isolate_ineq_aoa_preds_per_datatype(ineqaoa_preds)
        # self.logger.debug(self.arithmetic_ineq_predicates)
        # self.logger.debug(filtered_dict)
        E = []
        for key in filtered_dict:
            edge_set = []
            ineq_group = filtered_dict[key]
            C_E = create_attrib_set_from_filter_predicates(ineq_group)
            self.create_dashed_edges(ineq_group, key, edge_set)
            add_concrete_bounds_as_edge(ineq_group, edge_set)
            # self.logger.debug("Directed edge: ", edge_set)
            unvisited = copy.deepcopy(C_E)
            # self.logger.debug("unvisited: ", unvisited)
            # while unvisited:
            for attrib in unvisited:
                # self.logger.debug("attrib:", attrib)
                C_next = get_out_edges(edge_set, attrib)
                # self.logger.debug(attrib, C_next)
                for c_attrib in C_next:
                    v_lj_prev = create_v_lj_2(ineq_group, c_attrib)
                    # self.logger.debug("LB prev:", c_attrib, v_lj_prev)
                    UB_impact = self.is_impacted_by_Bound(attrib, c_attrib, ineq_group, key, query, v_lj_prev, True)
                    LB_impact = self.is_impacted_by_Bound(attrib, c_attrib, ineq_group, key, query, v_lj_prev, False)
                    adjust_Bounds(LB_impact, UB_impact,
                                  absorbed_LBs, absorbed_UBs,
                                  aoa_CB_LBs, aoa_CB_UBs,
                                  attrib, c_attrib, edge_set)
            left_over_aoa_CBs(absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs, edge_set)
            E.extend(edge_set)
        return E, (absorbed_LBs, absorbed_UBs, aoa_CB_LBs, aoa_CB_UBs)

    def is_impacted_by_Bound(self, attrib, c, ineq_group, key, query, v_lj_prev, is_UB):
        self.mutate_attrib_with_B(attrib, key, ineq_group, is_UB)
        f_e = self.do_bound_check_again(c, key, query)
        self.logger.debug(c, f_e)
        if len(f_e) > 0 and v_lj_prev != f_e[0][3]:
            _impact = True
        else:
            _impact = False
        return _impact

    def create_dashed_edges(self, ineq_group, key, edge_set):
        seq = get_all_two_combs(ineq_group)
        for e in seq:
            one, two = e[0], e[1]
            self.create_dashed_edge_from_oneTotwo(edge_set, key, one, two)
            self.create_dashed_edge_from_oneTotwo(edge_set, key, two, one)

    def create_dashed_edge_from_oneTotwo(self, edge_set, key, one, two):
        tab_attrib = (one[0], one[1])
        peer_tab_attrib = (two[0], two[1])
        check = self.is_dmin_val_lessEqualTo_as_B(one, two)
        self.logger.debug(check)
        if check:
            edge_set.append(tuple([peer_tab_attrib, tab_attrib]))

    def isolate_ineq_aoa_preds_per_datatype(self, ineqaoa_preds):
        datatype_dict = {}
        for pred in ineqaoa_preds:
            tab_attrib = (pred[0], pred[1])
            datatype = self.filter_extractor.get_datatype(tab_attrib)
            if datatype in datatype_dict.keys():
                datatype_dict[datatype].append(pred)
            else:
                datatype_dict[datatype] = [pred]
        filtered_dict = {key: value for key, value in datatype_dict.items() if len(value) > 1}
        self.arithmetic_ineq_predicates = []
        for key in datatype_dict:
            if len(datatype_dict[key]) == 1:
                self.arithmetic_ineq_predicates.extend(datatype_dict[key])
        return filtered_dict

    def find_eq_join_graph(self, query, partition_eq_dict):
        self.algebraic_eq_predicates = []
        self.logger.debug(partition_eq_dict)
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
        self.logger.debug(self.algebraic_eq_predicates)

    def handle_unit_eq_group(self, equi_join_group, query):
        filter_attribs = []
        datatype = self.filter_extractor.get_datatype(equi_join_group[0])
        prepared_attrib_list = self.filter_extractor.prepare_attrib_set_for_bulk_mutation(equi_join_group)
        self.filter_extractor.extract_filter_on_attrib_set(filter_attribs, query, prepared_attrib_list,
                                                           datatype)
        if len(filter_attribs) > 0:
            if filter_attribs[0][2] == '=' or filter_attribs[0][2] == 'equal':
                return False
            equi_join_group.extend(filter_attribs)
        self.algebraic_eq_predicates.append(equi_join_group)
        return True

    def preprocess_for_aeqa(self):
        partition_eq_dict = {}
        others = []
        for pred in self.filter_extractor.filter_predicates:
            if pred[2] == '=' or pred[2] == 'equal':
                dict_key = pred[3]
                if dict_key in partition_eq_dict:
                    partition_eq_dict[dict_key].append((pred[0], pred[1]))
                else:
                    partition_eq_dict[dict_key] = [(pred[0], pred[1])]
            else:
                others.append(pred)

        self.arithmetic_eq_predicates = []
        for key in partition_eq_dict.keys():
            if len(partition_eq_dict[key]) == 1:
                tab = partition_eq_dict[key][0][0]
                col = partition_eq_dict[key][0][1]
                val = key
                self.arithmetic_eq_predicates.append((tab, col, '=', val, val))
        self.logger.debug(self.arithmetic_eq_predicates)
        filtered_dict = {key: value for key, value in partition_eq_dict.items() if len(value) > 1}
        return filtered_dict, others

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

    def mutate_attrib(self, attrib, c, datatype, ineq_group):
        _UB, _LB = 0, 0
        for pred in ineq_group:
            if attrib[0] == pred[0] and attrib[1] == pred[1]:
                _UB = pred[4]
        for pred in ineq_group:
            if c[0] == pred[0] and c[1] == pred[1]:
                _LB = pred[3]
        mutate_with = get_mid_val(datatype, _UB, _LB)

        if datatype == 'date':
            mutate_with = f"\'{mutate_with}\'"
        self.connectionHelper.execute_sql([f"update {attrib[0]} set {attrib[1]} = {mutate_with};"])

    def mutate_attrib_with_B(self, attrib, datatype, ineq_group, is_UB):
        _UB = 0
        for pred in ineq_group:
            if attrib[0] == pred[0] and attrib[1] == pred[1]:
                if is_UB:
                    _UB = pred[4]
                else:
                    _UB = pred[3]
        mutate_with = _UB

        if datatype == 'date':
            mutate_with = f"\'{mutate_with}\'"
        self.connectionHelper.execute_sql([f"update {attrib[0]} set {attrib[1]} = {mutate_with};"])

    def optimize_aoa_predicates(self):
        seqs = get_all_two_combs(self.aoa_predicates)
        to_remove = []
        for seq in seqs:
            if isinstance(seq[0], tuple) and isinstance(seq[1], tuple):
                if seq[0][1] == seq[1][0] and (seq[0][0], seq[1][1]) in self.aoa_predicates:
                    to_remove.append((seq[0][0], seq[1][1]))
        for t_r in to_remove:
            self.aoa_predicates.remove(t_r)

    def find_cb_ubs(self, a_UBs, aoa_UBs, path, query):
        cb_UBs = self.check_for_CB_UB(path, a_UBs, aoa_UBs, query)
        for cb_ub in cb_UBs:
            datatype = self.filter_extractor.get_datatype((cb_ub[0], cb_ub[1]))
            _, i_max = get_min_and_max_val(datatype)
            if cb_ub[4] != i_max:
                self.aoa_predicates.append([(cb_ub[0], cb_ub[1]), cb_ub[4]])
        self.logger.debug("cb_LBs:", cb_UBs)
