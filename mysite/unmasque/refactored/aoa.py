from mysite.unmasque.refactored.abstract.where_clause import WhereClause
from mysite.unmasque.refactored.filter import Filter, get_constants_for


def get_all_two_combs(items):
    seq = []
    for i in range(0, len(items)):
        for j in range(i + 1, len(items)):
            print(f"{str(i)} {str(j)}")
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


class AlgebraicPredicate(WhereClause):
    def __init__(self, connectionHelper, global_key_lists,
                 core_relations, global_min_instance_dict):
        super().__init__(connectionHelper, global_key_lists, core_relations, global_min_instance_dict,
                         "AlgebraicPredicate")
        self.filter_extractor = Filter(self.connectionHelper, global_key_lists,
                                       core_relations, global_min_instance_dict)
        self.aoa_predicates = None
        self.aeq_predicates = None
        self.arithmetic_eq_predicates = None
        self.algebraic_eq_predicates = None
        self.arithmetic_ineq_predicates = None

    def doActualJob(self, args):
        self.filter_extractor.mock = self.mock
        query = self.extract_params_from_args(args)
        check = self.filter_extractor.doJob(query)
        if not check:
            return False
        partition_eq_dict, ineqaoa_preds = self.preprocess_for_aeqa()
        self.find_eq_join_graph(query, partition_eq_dict)
        self.find_ineq_aoa(query, ineqaoa_preds)
        return True

    def is_dmin_val_same_as_B(self, pred, is_UB, datatype, peer_tab_attrib):
        tab, attrib = pred[0], pred[1]
        peer_tab, peer_attrib = peer_tab_attrib[0], peer_tab_attrib[1]
        _B = pred[4] if is_UB else pred[3]
        values = self.global_min_instance_dict[peer_tab]
        attribs, vals = values[0], values[1]
        attrib_idx = attribs.index(peer_attrib)
        val = vals[attrib_idx]
        self.logger.debug(f"{tab}.{attrib} vs. {peer_tab}.{peer_attrib}: {str(val)}, {str(_B)}")

        if datatype == 'numeric':
            delta, _ = get_constants_for(datatype)
            result = max(_B, val) - min(_B, val)
            return result <= delta
        return val == _B

    def find_ineq_aoa(self, query, ineqaoa_preds):
        filtered_dict = self.isolate_ineq_aoa_preds_per_datatype(ineqaoa_preds)
        self.logger.debug(self.arithmetic_ineq_predicates)
        self.logger.debug(filtered_dict)
        for key in filtered_dict:
            ineq_group = filtered_dict[key]
            edge = self.create_dashed_edges(ineq_group, key)
            self.logger.debug("Directed edge: ", edge)

    def create_dashed_edges(self, ineq_group, key):
        seq = get_all_two_combs(ineq_group)
        edge_set = []
        for e in seq:
            one, two = e[0], e[1]
            self.create_dashed_edge_from_oneTotwo(edge_set, key, one, two)
            self.create_dashed_edge_from_oneTotwo(edge_set, key, two, one)
        return edge_set

    def create_dashed_edge_from_oneTotwo(self, edge_set, key, one, two):
        tab_attrib = (one[0], one[1])
        peer_tab_attrib = (two[0], two[1])
        check = self.is_dmin_val_same_as_B(one, False, key, peer_tab_attrib)
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
                self.arithmetic_ineq_predicates.append(datatype_dict[key])
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
