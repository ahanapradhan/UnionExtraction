from mysite.unmasque.src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase
from mysite.unmasque.src.core.result_comparator import ResultComparator


class HiddenAggregate(GenerationPipeLineBase):

    def __init__(self, connectionHelper, delivery):
        super().__init__(connectionHelper, "Nested Aggregated", delivery)
        self.comparator = ResultComparator(self.connectionHelper, True, delivery.core_relations)

    def assertEqAtSingleRowDmin(self, q_h, q_e):
        self.restore_d_min_from_dict()
        check = self.comparator.match(q_h, q_e)
        return check

    def doActualJob(self, args=None):
        query, Q_E = self.extract_params_from_args(args)
        check = self.assertEqAtSingleRowDmin(query, Q_E)
        if check:
            self.logger.info(" It may be a nested aggregate. ")
        return check

    def extract_params_from_args(self, args):
        query, Q_E = args[0][0], args[0][1]
        return query, Q_E
