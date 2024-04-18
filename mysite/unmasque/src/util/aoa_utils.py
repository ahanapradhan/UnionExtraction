import datetime
from datetime import date
from typing import Union

from .utils import get_datatype_of_val, get_format


def optimize_edge_set(edge_set: list[tuple[tuple[str, str], tuple[str, str]]]):
    nodes = set()
    for pred in edge_set:
        nodes.add(pred[0])
        nodes.add(pred[1])
    C_E = list(nodes)
    w, h = len(C_E), len(C_E)
    m = optimize_by_matrix(C_E, edge_set, h, w)
    for x in range(h):
        for c in range(w):
            if not m[x][c]:
                for aoa in edge_set:
                    if aoa[0] == C_E[x] and aoa[1] == C_E[c]:
                        edge_set.remove(aoa)


def is_sublist(lst, sublist):
    lst_hash = [hash(el) for el in lst]
    sublist_hash = [hash(el) for el in sublist]

    seq = []
    for s in sublist_hash:
        if s in lst_hash:
            s_idx = lst_hash.index(s)
            seq.append(s_idx)
    if not len(seq) or len(seq) != len(sublist_hash):
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
        try:
            chains.remove(r)
        except ValueError:
            pass
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


def get_all_two_combs2(items):
    seq = []
    for i in range(0, len(items)):
        for j in range(i + 1, len(items)):
            seq.append([items[i], items[j]])
            seq.append([items[j], items[i]])
    return seq


def get_LB(pred: tuple[str, str, str, Union[int, float, date], Union[int, float, date]]):
    return pred[3]


def get_UB(pred: tuple[str, str, str, Union[int, float, date], Union[int, float, date]]):
    return pred[4]


def get_attrib(pred: Union[
    tuple[str, str, str, Union[int, float, date], Union[int, float, date]],
    tuple[str, str]
]):
    return pred[1]


def get_tab(pred: Union[
    tuple[str, str, str, Union[int, float, date], Union[int, float, date]],
    tuple[str, str]
]):
    return pred[0]


def get_op(pred: tuple[str, str, str, Union[int, float, date], Union[int, float, date]]):
    return pred[2]


def get_max(pred: tuple[Union[int, float, date], Union[int, float, date], Union[int, float, date]]):
    return pred[1]


def get_min(pred: tuple[Union[int, float, date], Union[int, float, date], Union[int, float, date]]):
    return pred[0]


def get_delta(pred: tuple[any, any, any]):
    return pred[2]


def create_attrib_set_from_filter_predicates(ineq_group):
    C_E = set()
    for pred in ineq_group:
        C_E.add((get_tab(pred), get_attrib(pred)))
    return list(C_E)


def create_adjacency_map_from_aoa_predicates(aoa_preds, skip_tuples=True) -> dict:
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

    def dp(index, subsets, _min_elements):
        if index == n:
            key = tuple(tuple(sorted(subset)) for subset in subsets)
            if all(len(subset) >= _min_elements for subset in subsets):
                return [subsets] if key not in memo else []
            else:
                return []

        key = (index, tuple(tuple(sorted(subset)) for subset in subsets))
        if key in memo:
            return memo[key]

        result = []

        for i, subset in enumerate(subsets):
            new_subsets = subsets[:i] + [subset + [arr[index]]] + subsets[i + 1:]
            result += dp(index + 1, new_subsets, _min_elements)

        if len(subsets) < _min_elements:
            result += dp(index + 1, subsets + [[arr[index]]], _min_elements)

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


def is_same_tab_attrib(one: Union[tuple[str, str], Union[int, float, date]],
                       two: Union[tuple[str, str], Union[int, float, date]]) -> bool:
    check = False
    try:
        check = get_tab(one) == get_tab(two) and get_attrib(one) == get_attrib(two)
    except TypeError:
        pass
    return check


def add_concrete_bounds_as_edge(pred_list, edge_set):
    for pred in pred_list:
        if pred[2] == '<=':
            edge_set.append([(get_tab(pred), get_attrib(pred)), get_UB(pred)])
        elif pred[2] == '>=':
            edge_set.append([get_LB(pred), (get_tab(pred), get_attrib(pred))])
        elif pred[2] == 'range':
            edge_set.append([get_LB(pred), (get_tab(pred), get_attrib(pred))])
            edge_set.append([(get_tab(pred), get_attrib(pred)), get_UB(pred)])


def add_concrete_bounds_as_edge2(pred_list, edge_set, datatype):
    for pred in pred_list:
        ub, lb = get_UB(pred), get_LB(pred)
        # if datatype == 'numeric':
        #    ub, lb = round(get_UB(pred), 2), round(get_LB(pred), 2)
        if pred[2] == '<=':
            edge_set.append(((get_tab(pred), get_attrib(pred)), ub))
        elif pred[2] == '>=':
            edge_set.append((lb, (get_tab(pred), get_attrib(pred))))
        elif pred[2] == 'range':
            edge_set.append((lb, (get_tab(pred), get_attrib(pred))))
            edge_set.append(((get_tab(pred), get_attrib(pred)), ub))


def get_LB_of_next_attrib(ineq_group: list[tuple], c: tuple[str, str]):
    for pred in ineq_group:
        if get_tab(pred) == get_tab(c) and get_attrib(pred) == get_attrib(c) and (get_op(pred) in ['>=', 'range']):
            return get_LB(pred)


def add_pred_for(aoa_l, pred):
    if isinstance(aoa_l, list) or isinstance(aoa_l, tuple):
        pred.append(f"{aoa_l[0]}.{aoa_l[1]}")
    else:
        pred.append(get_format(get_datatype_of_val(aoa_l), aoa_l))
    return aoa_l


def adjust_Bounds2(LB_impact: bool, UB_impact: bool,
                   absorbed_LBs, absorbed_UBs,
                   aoa_CB_LBs, aoa_CB_UBs,
                   attrib: tuple[str, str], next_attrib: tuple[str, str],
                   edge_set: list[tuple[tuple[str, str], tuple[str, str]]]) -> bool:
    _LB = get_concrete_LB_in_edge(edge_set, next_attrib)
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
        edge_set.remove((attrib, next_attrib))
        return False
    return True


def get_val_bound_for_chain(i_min, i_max, filter_attrib, is_UB):
    if not len(filter_attrib):
        if is_UB:
            val = i_max
        else:
            val = i_min
    else:
        if is_UB:
            val = get_UB(filter_attrib[0])
        else:
            val = get_LB(filter_attrib[0])
    return val


def get_min_max_for_chain_bounds(i_min, i_max, tab_attrib, a_b, is_UB):
    if is_UB:
        if tab_attrib in a_b.keys():
            min_val = a_b[tab_attrib][0]
        else:
            min_val = i_min
        max_val = i_max
    else:
        if tab_attrib in a_b.keys():
            max_val = a_b[tab_attrib][0]
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


def remove_item_from_list(item, item_list):
    try:
        item_list.remove(item)
    except ValueError:
        pass


def find_le_attribs_from_edge_set(attrib: tuple[str, str], edge_set: list) -> list:
    prev_lb_attrib = map(lambda x: x[0]
    if is_same_tab_attrib(x[1], attrib) and isinstance(x[0], tuple)
    else None, edge_set)
    prev_lb_list = list(filter(lambda lb: lb is not None, prev_lb_attrib))
    return prev_lb_list


def find_ge_attribs_from_edge_set(attrib: tuple[str, str], edge_set: list) -> list:
    prev_ub_attrib = map(lambda x: x[1]
    if is_same_tab_attrib(x[0], attrib) and isinstance(x[1], tuple)
    else None, edge_set)
    prev_ub_list = list(filter(lambda ub: ub is not None, prev_ub_attrib))
    return prev_ub_list


def add_item_to_list(item, item_list):
    if item not in item_list:
        item_list.append(item)


def add_item_to_dict(_dict: dict, key, item):
    if key not in _dict.keys():
        _dict[key] = [item]
    else:
        _dict[key].append(item)


def remove_absorbed_Bs(E, absorbed_LBs, absorbed_UBs, col_sink, col_src):
    if col_src in absorbed_UBs.keys():
        for item in absorbed_UBs[col_src]:
            remove_item_from_list((col_src, item), E)
    if col_src in absorbed_LBs.keys():
        for item in absorbed_LBs[col_src]:
            remove_item_from_list((item, col_src), E)
    if col_sink in absorbed_LBs.keys():
        for item in absorbed_LBs[col_sink]:
            remove_item_from_list((item, col_sink), E)
    if col_sink in absorbed_UBs.keys():
        for item in absorbed_UBs[col_sink]:
            remove_item_from_list((col_sink, item), E)


def remove_all_absorbed_Bs(E, absorbed_LBs, absorbed_UBs):
    for col_src in absorbed_UBs.keys():
        remove_item_from_list((col_src, absorbed_UBs[col_src]), E)
    for col_sink in absorbed_LBs:
        remove_item_from_list((absorbed_LBs[col_sink], col_sink), E)


def get_all_indices(item, item_list):
    idx = []
    for i in range(len(item_list)):
        if item_list[i] == item:
            idx.append(i)
    return idx


def get_all_elements(item_list, idx_list):
    items = []
    for idx in idx_list:
        items.append(item_list[idx])
    return items


def find_transitive_concrete_upperBs(E, to_remove):
    if not len(E):
        return
    ls, us = zip(*E)
    us_dict = {}
    for u in us:
        if isinstance(u, tuple):
            continue
        if u not in us_dict.keys():
            us_dict[u] = 1
        else:
            us_dict[u] += 1
    for u in us_dict.keys():
        if us_dict[u] > 1:
            idx = get_all_indices(u, us)
            ls_elms = get_all_elements(ls, idx)
            pairs = get_all_two_combs2(ls_elms)
            for pair in pairs:
                lesser, greater = pair[0], pair[1]
                if (lesser, greater) in E:
                    to_remove.append((lesser, u))


def find_transitive_concrete_lowerBs(E, to_remove):
    if not len(E):
        return
    ls, us = zip(*E)
    ls_dict = {}
    for lm in ls:
        if isinstance(lm, tuple):
            continue
        if lm not in ls_dict.keys():
            ls_dict[lm] = 1
        else:
            ls_dict[lm] += 1
    for lm in ls_dict.keys():
        if ls_dict[lm] > 1:
            idx = get_all_indices(lm, ls)
            us_elms = get_all_elements(us, idx)
            pairs = get_all_two_combs2(us_elms)
            for pair in pairs:
                lesser, greater = pair[0], pair[1]
                if (lesser, greater) in E:
                    to_remove.append((lm, greater))


def find_concrete_bound_from_filter_bounds(attrib, edge_set, prev_bound, is_upper_bound):
    if is_upper_bound:
        prev_attrib = map(lambda x: x[1]
        if is_same_tab_attrib(x[0], attrib) and not isinstance(x[1], tuple)
        else None, edge_set)
    else:
        prev_attrib = map(lambda x: x[0]
        if is_same_tab_attrib(x[1], attrib) and not isinstance(x[0], tuple)
        else None, edge_set)

    prev_list = list(filter(lambda b: b is not None, prev_attrib))
    if len(prev_list):
        prev_bound = prev_list[0]  # only one concrete bound possible
    return prev_bound


def do_numeric_drama(other_LB, datatype, my_val, delta, satisfied) -> bool:
    # all the following DRAMA is to handle "numeric" datatype
    if datatype == 'numeric':
        bck_diff_1 = float(my_val) - other_LB
        alt_sat = True
        if not satisfied:
            if bck_diff_1 > 0:
                alt_sat = alt_sat & (abs(bck_diff_1) <= delta)
        return alt_sat or satisfied
    return satisfied


def is_equal(one, other, datatype):
    if datatype == 'numeric':
        diff = abs(one - other)
        delta, _ = get_constants_for(datatype)
        return diff <= delta
    else:
        return one == other


def conseq(nums):
    for i in range(len(nums) - 1):
        if nums[i] == nums[i + 1]:
            return nums[i]

    # Only return False once we've exhausted all numbers.
    # Since we didn't return True so far - it means there are
    # no consecutive equal numbers, so we can safely return False
    return False


def need_permanent_mutation(datatype, diffs: list) -> bool:
    delta, cutoff = get_constants_for(datatype)
    if datatype == 'date':
        d_val = delta * datetime.timedelta(days=1)
    elif datatype in ['int', 'numeric']:
        d_val = delta
    else:
        raise ValueError
    val = conseq(diffs)
    return val == d_val


def get_constants_for(datatype):
    if datatype in ('int', 'date', 'number'):
        while_cut_off = 0
        delta = 1
    elif datatype in ('float', 'numeric'):
        while_cut_off = 0.00
        delta = 0.01
    else:
        raise ValueError(f"Unsupported datatype: {datatype}")
    return delta, while_cut_off
