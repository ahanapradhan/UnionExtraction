from abc import ABC, abstractmethod

from mysite.unmasque.src.experimental.where_aggregate import HiddenAggregate
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine


class NestedAggWherePipeLine(ExtractionPipeLine):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Nested Aggregate PipeLine")
        self.all_relations = None
        self.pipeLineError = False

    def _after_from_clause_extract(self, query, core_relations):
        eq, t = super()._after_from_clause_extract(query, core_relations)
        self.time_profile.update(t)

        eq = self._extract_nested_aggregate(eq, query)
        return eq, self.time_profile

    def _extract_nested_aggregate(self, eq, query):
        ha = HiddenAggregate(self.connectionHelper, self.genPipelineCtx, self.global_pk_dict, self.q_generator)
        ha.enabled = self.connectionHelper.config.detect_nested
        check = ha.doJob([query, eq])
        self.time_profile.update_for_correlated_nested_subquery(ha.local_elapsed_time, ha.app_calls)
        if check:
            self.logger.debug("OK")
        else:
            self.logger.debug("Oops!")
        return ha.Q_E
