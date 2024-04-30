from abc import ABC, abstractmethod

from mysite.unmasque.src.core.where_aggregate import HiddenAggregate
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine


class NestedAggWherePipeLine(GenericPipeLine, ABC):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Nested Aggregate")

    @abstractmethod
    def extract(self, query):
        pass

    @abstractmethod
    def process(self, query: str):
        raise NotImplementedError("Trouble!")

    @abstractmethod
    def doJob(self, query, qe=None):
        raise NotImplementedError("Trouble!")

    @abstractmethod
    def verify_correctness(self, query, result):
        raise NotImplementedError("Trouble!")

    def _extract_nested_aggregate(self, eq, q_generator, query, delivery, global_pk_dict):
        ha = HiddenAggregate(self.connectionHelper, delivery, global_pk_dict, q_generator)
        check = ha.doJob([query, eq])
        self.time_profile.update_for_correlated_nested_subquery(ha.local_elapsed_time, ha.app_calls)
        if check:
            self.logger.debug("OK")
        else:
            self.logger.debug("Oops!")
        return ha.inner_query
