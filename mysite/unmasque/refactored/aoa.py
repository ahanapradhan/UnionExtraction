from mysite.unmasque.refactored.abstract.where_clause import WhereClause
from mysite.unmasque.refactored.filter import Filter


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

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        check = self.filter_extractor.doJob(query)
        if not check:
            return False
        self.find_eq_join_graph(query)
        return True

    def find_eq_join_graph(self, query):
        self.algebraic_eq_predicates = []
        partition_eq_dict = self.preprocess_for_aeqa()
        self.logger.debug(partition_eq_dict)
        for key in partition_eq_dict.keys():
            filter_attribs = []
            datatype = self.filter_extractor.get_datatype(partition_eq_dict[key][0])
            prepared_attrib_list = self.filter_extractor.prepare_attrib_set_for_bulk_mutation(partition_eq_dict[key])
            self.filter_extractor.extract_filter_on_attrib_set(filter_attribs, query, prepared_attrib_list, datatype)
            if filter_attribs:
                partition_eq_dict[key].extend(filter_attribs)
            self.algebraic_eq_predicates.append(partition_eq_dict[key])
        self.logger.debug(self.algebraic_eq_predicates)

    def preprocess_for_aeqa(self):
        partition_eq_dict = {}
        for pred in self.filter_extractor.filter_predicates:
            if pred[2] == '=' or pred[2] == 'equal':
                dict_key = pred[3]
                if dict_key in partition_eq_dict:
                    partition_eq_dict[dict_key].append((pred[0], pred[1]))
                else:
                    partition_eq_dict[dict_key] = [(pred[0], pred[1])]

        self.arithmetic_eq_predicates = []
        for key in partition_eq_dict.keys():
            if len(partition_eq_dict[key]) == 1:
                tab = partition_eq_dict[key][0][0]
                col = partition_eq_dict[key][0][1]
                val = key
                self.arithmetic_eq_predicates.append((tab, col, '=', val, val))
        self.logger.debug(self.arithmetic_eq_predicates)
        filtered_dict = {key: value for key, value in partition_eq_dict.items() if len(value) > 1}
        return filtered_dict
