from mysite.unmasque.refactored.abstract.where_clause import WhereClause


class AlgebraicPredicate(WhereClause):
    def __init__(self, connectionHelper, global_key_lists,
                 core_relations, global_min_instance_dict, filter_predicates):
        super().__init__(connectionHelper,
                         global_key_lists,
                         core_relations,
                         global_min_instance_dict)
        self.filter_predicates = filter_predicates
        self.aoa_predicates = None
        self.aeq_predicates = None

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        return True

