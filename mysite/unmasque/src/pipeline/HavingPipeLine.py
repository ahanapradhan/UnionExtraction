from abc import ABC

from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper
from mysite.unmasque.src.core.cs2 import Cs2
from mysite.unmasque.src.core.db_restorer import DbRestorer
from mysite.unmasque.src.core.from_clause import FromClause
from mysite.unmasque.src.util.constants import *
from ...src.pipeline.abstract.generic_pipeline import GenericPipeLine

class HavingPipeline(GenericPipeLine, ABC):
    def __init__(self, connectionHelper: AbstractConnectionHelper):
        super().__init__(connectionHelper, "Having PipeLine")
        self.query: str | None = None

    def process(self, query: str):
        return GenericPipeLine.process(self, query)

    def extract(self, query: str):
        # Establish connection to db
        self.connectionHelper.connectUsingParams()
        self.query = query

        # *** - Pipeline operations begin here - ***

        self._from_clause_extraction()
        self._correlated_sampling()

        # *** - Pipeline operations end here   - ***

    def _from_clause_extraction(self):
        """
            Helper function to run the From clause extraction routine.
            Does not require a minimized database.
        """
        self.update_state(FROM_CLAUSE + START)
        fc = FromClause(self.connectionHelper)
        self.update_state(FROM_CLAUSE + RUNNING)
        check = fc.doJob(self.query)
        self.update_state(FROM_CLAUSE + DONE)
        self.time_profile.update_for_from_clause(fc.local_elapsed_time, fc.app_calls)

        if not check or not fc.done:
            self.logger.error("Some problem while extracting from clause. Aborting!")
            self.core_relations= None
            self.info[FROM_CLAUSE] = None
            return None, self.time_profile

        self.core_relations = fc.core_relations
        self.all_sizes = fc.init.all_sizes
        self.key_lists = fc.get_key_lists()
        self.global_pk_dict = fc.init.global_pk_dict
        self.info[FROM_CLAUSE] = fc.core_relations
        self.logger.debug(f"Core relations: {self.core_relations}")

        # Restore back the database to its original state
        self.update_state(RESTORE_DB + START)
        self.db_restorer = DbRestorer(self.connectionHelper, self.core_relations)
        self.db_restorer.set_all_sizes(self.all_sizes)
        self.update_state(RESTORE_DB + RUNNING)
        check = self.db_restorer.doJob(None)
        self.update_state(RESTORE_DB + DONE)

        self.time_profile.update_for_db_restore(self.db_restorer.local_elapsed_time, self.db_restorer.app_calls)
        if not check or not self.db_restorer.done:
            self.info[RESTORE_DB] = None
            self.logger.info("DB restore failed!")
        self.info[RESTORE_DB] = {'size': self.db_restorer.last_restored_size}
        print('DB restored')

    def _correlated_sampling(self):
        """
            Helper function to run the correlated sampling routine.
            Does not require a minimized database.
        """
        self.update_state(SAMPLING + START)
        cs2 = Cs2(self.connectionHelper, self.db_restorer.last_restored_size, self.core_relations, self.key_lists)
        self.update_state(SAMPLING + RUNNING)
        check = cs2.doJob(self.query)
        self.update_state(SAMPLING + DONE)

        self.time_profile.update_for_cs2(cs2.local_elapsed_time, cs2.app_calls)
        if not check or not cs2.done:
            self.info[SAMPLING] = None
            self.logger.info("Sampling failed!")
        else:
            self.info[SAMPLING] = {'sample': cs2.sample, 'size': cs2.sizes}
            self.logger.info(f"Sampling successful! Info {self.info[SAMPLING]}")

