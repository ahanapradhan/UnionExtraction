import copy
from abc import abstractmethod, ABC

from ....src.core.aoa import InequalityPredicate
from ....src.core.cs2 import Cs2
from ....src.core.db_restorer import DbRestorer
from ....src.core.equi_join import U2EquiJoin
from ....src.core.filter import Filter
from ....src.core.view_minimizer import ViewMinimizer
from ....src.pipeline.abstract.generic_pipeline import GenericPipeLine
from ....src.util.aoa_utils import get_constants_for
from ....src.util.constants import FILTER, INEQUALITY, DONE, RUNNING, START, EQUALITY, DB_MINIMIZATION, \
    SAMPLING, RESTORE_DB, ERROR
from ....src.util.utils import get_format, get_val_plus_delta


def get_eq_filters(arithmetics):
    return [pred for pred in arithmetics if pred[2] in ['equal', '=']]


class DisjunctionPipeLine(GenericPipeLine, ABC):

    def __init__(self, connectionHelper, name):
        GenericPipeLine.__init__(self, connectionHelper, name)
        self.aoa = None
        self.equi_join = None
        self.filter_extractor = None
        self.db_restorer = None
        self.global_min_instance_dict = None
        self.key_lists = None

    def _mutation_pipeline(self, core_relations, query, time_profile, restore_details=None):
        self.update_state(RESTORE_DB + START)
        self.db_restorer = DbRestorer(self.connectionHelper, core_relations)
        self.db_restorer.set_all_sizes(self.all_sizes)
        self.update_state(RESTORE_DB + RUNNING)
        check = self.db_restorer.doJob(restore_details)
        self.update_state(RESTORE_DB + DONE)
        time_profile.update_for_db_restore(self.db_restorer.local_elapsed_time, self.db_restorer.app_calls)
        if not check or not self.db_restorer.done:
            self.info[RESTORE_DB] = None
            self.logger.info("DB restore failed!")
            return False, time_profile
        self.info[RESTORE_DB] = {'size': self.db_restorer.last_restored_size}

        """
        Correlated Sampling
        """
        self.update_state(SAMPLING + START)
        cs2 = Cs2(self.connectionHelper, self.all_sizes, core_relations, self.key_lists)
        self.update_state(SAMPLING + RUNNING)
        check = cs2.doJob(query)
        self.update_state(SAMPLING + DONE)
        time_profile.update_for_cs2(cs2.local_elapsed_time, cs2.app_calls)
        if not check or not cs2.done:
            self.info[SAMPLING] = None
            self.logger.info("Sampling failed!")
        if not self.connectionHelper.config.use_cs2:
            self.info[SAMPLING] = SAMPLING + "DISABLED"
            self.logger.info("Sampling is disabled!")
        else:
            self.info[SAMPLING] = {'sample': cs2.sample, 'size': cs2.sizes}

        """
            View based Database Minimization
            """
        self.update_state(DB_MINIMIZATION + START)
        vm = ViewMinimizer(self.connectionHelper, core_relations, self.db_restorer.last_restored_size, cs2.passed)
        self.update_state(DB_MINIMIZATION + RUNNING)
        check = vm.doJob(query)
        self.update_state(DB_MINIMIZATION + DONE)
        time_profile.update_for_view_minimization(vm.local_elapsed_time, vm.app_calls)
        if not check or not vm.done:
            self.error = "Cannot do database minimization. "
            self.logger.error(self.error)
            self.update_state(ERROR)
            self.info[DB_MINIMIZATION] = None
            return False, time_profile
        self.db_restorer.update_last_restored_size(vm.all_sizes)
        self.info[DB_MINIMIZATION] = vm.global_min_instance_dict
        self.global_min_instance_dict = copy.deepcopy(vm.global_min_instance_dict)

        '''
        Constant Filter Extraction
        '''
        self.update_state(FILTER + START)
        self.filter_extractor = Filter(self.connectionHelper, core_relations, self.global_min_instance_dict)
        self.update_state(FILTER + RUNNING)
        check = self.filter_extractor.doJob(query)
        self.update_state(FILTER + DONE)
        time_profile.update_for_where_clause(self.filter_extractor.local_elapsed_time,
                                             self.filter_extractor.app_calls)
        if not self.filter_extractor.done:
            self.update_state(ERROR)
            self.info[FILTER] = None
            self.error = "Some problem in filter extraction!"
            self.logger.error(self.error)
            return False, time_profile
        if not check:
            self.info[FILTER] = None
            self.logger.info("No filter found")
        self.info[FILTER] = self.filter_extractor.filter_predicates

        '''
        Equality Relations (Equi-join + Constant Equality filters) Extraction
        '''
        self.update_state(EQUALITY + START)
        self.update_state(EQUALITY + RUNNING)
        self.equi_join = U2EquiJoin(self.connectionHelper, core_relations, self.filter_extractor.filter_predicates,
                                    self.filter_extractor, self.global_min_instance_dict)
        check = self.equi_join.doJob(query)
        self.update_state(EQUALITY + DONE)
        time_profile.update_for_where_clause(self.equi_join.local_elapsed_time, self.equi_join.app_calls)
        if not self.equi_join.done:
            self.update_state(ERROR)
            self.info[EQUALITY] = None
            self.error = "Some problem in Equality predicate extraction!"
            self.logger.error(self.error)
            return False, time_profile
        if not check:
            self.info[EQUALITY] = None
            self.logger.info("No Equality predicate found")
        combined_eq_predicates = self.equi_join.algebraic_eq_predicates + self.equi_join.arithmetic_eq_predicates
        self.info[EQUALITY] = combined_eq_predicates

        '''
        AOA Extraction
        '''
        self.update_state(INEQUALITY + START)
        self.aoa = InequalityPredicate(self.connectionHelper, core_relations, self.equi_join.pending_predicates,
                                       self.equi_join.arithmetic_eq_predicates,
                                       self.equi_join.algebraic_eq_predicates, self.filter_extractor,
                                       self.global_min_instance_dict)
        self.update_state(INEQUALITY + RUNNING)
        check = self.aoa.doJob(query)
        self.update_state(INEQUALITY + DONE)
        time_profile.update_for_where_clause(self.aoa.local_elapsed_time, self.aoa.app_calls)
        self.info[INEQUALITY] = self.aoa.aoa_predicates + self.aoa.aoa_less_thans + self.aoa.arithmetic_ineq_predicates
        if not check:
            self.info[INEQUALITY] = None
            self.logger.info("Cannot find inequality Predicates.")
        if not self.aoa.done:
            self.info[INEQUALITY] = None
            self.error = "Some error while Inequality Predicates extraction. Aborting extraction!"
            self.logger.error(self.error)
            self.update_state(ERROR)
            return False, time_profile
        return True, time_profile

    def __get_predicates_in_action(self):
        return self.aoa.arithmetic_filters

    @abstractmethod
    def process(self, query: str):
        raise NotImplementedError("Trouble!")

    @abstractmethod
    def doJob(self, query, qe=None):
        raise NotImplementedError("Trouble!")

    @abstractmethod
    def verify_correctness(self, query, result):
        raise NotImplementedError("Trouble!")

    def _extract_disjunction(self, init_predicates, core_relations, query, time_profile):  # for once
        self.or_predicates = []
        curr_eq_predicates = copy.deepcopy(init_predicates)
        all_eq_predicates = [curr_eq_predicates]
        ids = list(range(len(curr_eq_predicates)))
        if self.connectionHelper.config.detect_or:
            try:
                time_profile = self.__run_extraction_loop(all_eq_predicates, core_relations, ids, query, time_profile)
            except Exception as e:
                self.update_state(ERROR)
                self.logger.error("Error in disjunction loop. ", str(e))
                return False, time_profile
        self.or_predicates = list(zip(*all_eq_predicates))
        return True, time_profile

    def __run_extraction_loop(self, all_eq_predicates, core_relations, ids, query, time_profile):
        while True:
            or_eq_predicates = []
            for i in ids:
                in_candidates = [copy.deepcopy(em[i]) for em in all_eq_predicates]
                self.logger.debug("Checking OR predicate of ", in_candidates)
                if not len(in_candidates[-1]):
                    or_eq_predicates.append(tuple())
                    continue

                restore_details = self.__get_OR_db_restoration_details(core_relations, in_candidates)
                self.logger.debug(restore_details)
                check, time_profile = self._mutation_pipeline(core_relations, query, time_profile, restore_details)
                if not check or not self.__get_predicates_in_action():
                    or_eq_predicates.append(tuple())
                else:
                    or_eq_predicates.append(self.__get_predicates_in_action()[i])
                self.logger.debug("new or predicates...", all_eq_predicates, or_eq_predicates)
            if all(element == tuple() for element in or_eq_predicates):
                break
            all_eq_predicates.append(or_eq_predicates)
        return time_profile

    def __get_OR_db_restoration_details(self, core_relations, in_candidates):
        restore_details = []
        for tab in core_relations:
            where_condition = self.__falsify_predicates(tab, in_candidates)
            restore_details.append((tab, where_condition))
        return restore_details

    def __falsify_predicates(self, tabname, held_predicates):
        always = "true"
        where_condition = always
        wheres = []
        for pred in held_predicates:
            if not len(pred):
                return where_condition
            tab, attrib, op, lb, ub = pred[0], pred[1], pred[2], pred[3], pred[4]
            if tab != tabname:
                continue
            datatype = self.filter_extractor.get_datatype((tab, attrib))
            val_lb, val_ub = get_format(datatype, lb), get_format(datatype, ub)

            if op.lower() in ['equal', '=']:
                where_condition = f"{attrib} != {val_lb}"
            elif op.lower() == 'like':
                where_condition = f"{attrib} NOT LIKE {val_lb}"
            else:
                delta, _ = get_constants_for(datatype)
                val_lb_minus_one = get_format(datatype, get_val_plus_delta(datatype, lb, -1 * delta))
                val_ub_plus_one = get_format(datatype, get_val_plus_delta(datatype, ub, 1 * delta))
                where_condition = f"({attrib} <= {val_lb_minus_one} or {attrib} >= {val_ub_plus_one})"
            wheres.append(where_condition)
        where_condition = " and ".join(wheres) if len(wheres) else always
        self.logger.debug(where_condition)
        return where_condition

    @abstractmethod
    def extract(self, query):
        pass
