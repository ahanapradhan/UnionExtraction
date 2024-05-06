from abc import ABC

from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper
from mysite.unmasque.src.core.from_clause import FromClause
from mysite.unmasque.src.util.constants import *
from ...src.pipeline.abstract.generic_pipeline import GenericPipeLine

class HavingPipeline(GenericPipeLine, ABC):
    def __init__(self, connectionHelper: AbstractConnectionHelper):
        super().__init__(connectionHelper, "Having PipeLine")

    def process(self, query: str):
        return GenericPipeLine.process(self, query)

    def extract(self, query: str):
        # Establish connection to db
        self.connectionHelper.connectUsingParams()

        # *** - Pipeline operations begin here - ***

        self._from_clause_extraction(query)

        # *** - Pipeline operations end here   - ***

    def _from_clause_extraction(self, query: str):
        """
            Helper function to run the From clause extraction routine.
            Does not require a minimized database.
        """
        self.update_state(FROM_CLAUSE + START)
        fc = FromClause(self.connectionHelper)
        self.update_state(FROM_CLAUSE + RUNNING)
        check = fc.doJob(query)
        self.update_state(FROM_CLAUSE + DONE)
        self.time_profile.update_for_from_clause(fc.local_elapsed_time, fc.app_calls)

        if not check or not fc.done:
            self.logger.error("Some problem while extracting from clause. Aborting!")
            self.core_relations= None
            return None, self.time_profile

        self.core_relations = fc.core_relations
        self.logger.debug(f"Core relations: {self.core_relations}")

