from abc import ABC, abstractmethod

from mysite.unmasque.src.core.nep import NepMinimizer, NEP
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine
from mysite.unmasque.src.util.constants import FILTER, DONE, NEP_, RUNNING, START, DB_MINIMIZATION, RESULT_COMPARE


class NepPipeLine(GenericPipeLine, ABC):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "NEP Pipeline")

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

    def extract_NEP(self, core_relations, sizes, eq, q_generator, query, time_profile, delivery):
        if not self.connectionHelper.config.detect_nep:
            return eq

        nep_minimizer = NepMinimizer(self.connectionHelper, core_relations, sizes)
        nep_extractor = NEP(self.connectionHelper, delivery)

        for i in range(10):
            self.update_state(NEP_ + RESULT_COMPARE + START)
            self.update_state(NEP_ + RESULT_COMPARE + RUNNING)
            matched = nep_minimizer.match(query, eq)
            self.update_state(NEP_ + RESULT_COMPARE + DONE)
            if matched is None:
                self.logger.error("Extracted Query is not semantically correct!..not going to try to extract NEP!")
                return eq
            if matched:
                self.logger.info("No NEP!")
                return eq

            for tabname in self.core_relations:
                self.update_state(NEP_ + DB_MINIMIZATION + START)
                self.update_state(NEP_ + DB_MINIMIZATION + RUNNING)
                minimized = nep_minimizer.doJob((query, eq, tabname))
                if not minimized:
                    continue
                self.update_state(NEP_ + DB_MINIMIZATION + DONE)

                self.update_state(NEP_ + FILTER + START)
                self.update_state(NEP_ + FILTER + RUNNING)
                nep_filters = nep_extractor.doJob((query, tabname))
                self.logger.debug(f"Nep filters on {tabname}: {str(nep_filters)}")
                if nep_filters is None or not len(nep_filters):
                    self.logger.info("NEP does not exists.")
                else:
                    eq = q_generator.updateExtractedQueryWithNEPVal(query, nep_filters)
                self.update_state(NEP_ + FILTER + DONE)

        time_profile.update_for_view_minimization(nep_minimizer.local_elapsed_time, nep_minimizer.app_calls)
        time_profile.update_for_nep(nep_extractor.local_elapsed_time, nep_extractor.app_calls)
        self.logger.debug("returning..", eq)
        return eq
