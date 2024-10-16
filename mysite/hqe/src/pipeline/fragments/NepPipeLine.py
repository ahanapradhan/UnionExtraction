from abc import ABC, abstractmethod

from ....src.util.QueryStringGenerator import QueryStringGenerator
from ....src.core.nep import NepMinimizer, NEP
from ....src.pipeline.abstract.generic_pipeline import GenericPipeLine
from ....src.util.constants import FILTER, DONE, NEP_, RUNNING, START, DB_MINIMIZATION, RESULT_COMPARE, NEP_COMPARATOR


class NepPipeLine(GenericPipeLine, ABC):
    def __init__(self, connectionHelper, name="NEP PipeLine"):
        super().__init__(connectionHelper, name)
        self.NEP_CUTOFF = 10
        self.q_generator = QueryStringGenerator(self.connectionHelper)

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

    def _extract_NEP(self, core_relations, sizes, query, genCtx):
        eq = self.q_generator.write_query()
        if not self.connectionHelper.config.detect_nep:
            self.logger.info("NEP check is disabled by config.")
            return eq

        self.logger.debug(f"Core relations: {core_relations}")
        nep_minimizer = NepMinimizer(self.connectionHelper, core_relations, sizes)
        nep_extractor = NEP(self.connectionHelper, genCtx)

        for i in range(self.NEP_CUTOFF):
            self.update_state(NEP_ + RESULT_COMPARE + START)
            self.update_state(NEP_ + RESULT_COMPARE + RUNNING)
            matched = nep_minimizer.match(query, eq)
            self.update_state(NEP_COMPARATOR + DONE)
            if matched is None:
                eq = self.q_generator.rewrite_for_NEP()
                self.error = "Extracted Query is not semantically correct!..not going to try to extract NEP!"
                self.logger.error(self.error)
                return eq
            if matched:
                eq = self.q_generator.rewrite_for_NEP()
                self.logger.info("No NEP!")
                return eq

            for tabname in core_relations:
                self.logger.debug(f"checking for table {tabname}")
                self.update_state(DB_MINIMIZATION + START)
                self.update_state(DB_MINIMIZATION + RUNNING)
                minimized = nep_minimizer.doJob((query, eq, tabname))
                self.time_profile.update_for_view_minimization(nep_minimizer.local_elapsed_time, nep_minimizer.app_calls)
                if not minimized:
                    continue
                self.update_state(DB_MINIMIZATION + DONE)

                self.update_state(FILTER + START)
                self.update_state(FILTER + RUNNING)
                nep_filters = nep_extractor.doJob((query, tabname))
                self.logger.debug(f"Nep filters on {tabname}: {str(nep_filters)}")
                if nep_filters is None or not len(nep_filters):
                    self.logger.info("NEP does not exists.")
                else:
                    eq = self.q_generator.updateExtractedQueryWithNEPVal(query, nep_filters)
                self.update_state(FILTER + DONE)
                self.time_profile.update_for_nep(nep_extractor.local_elapsed_time, nep_extractor.app_calls)

        eq = self.q_generator.rewrite_for_NEP()
        self.logger.debug("returning..", eq)
        return eq
