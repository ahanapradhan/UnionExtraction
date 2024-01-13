from mysite.unmasque.refactored.abstract.where_clause import WhereClause
from mysite.unmasque.refactored.filter import Filter


class AlgebraicPredicate(WhereClause):
    def __init__(self, connectionHelper, global_key_lists,
                 core_relations, global_min_instance_dict):
        super().__init__(connectionHelper, global_key_lists, core_relations, global_min_instance_dict, "AlgebraicPredicate")
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
        self.find_eq_join_graph()
        return True

    def find_eq_join_graph(self):
        partition_eq_dict = self.preprocess_for_aeqa()
        self.logger.debug(partition_eq_dict)

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
