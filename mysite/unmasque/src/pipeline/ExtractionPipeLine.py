import copy

import mysite.unmasque.src.util.utils
from .DisjunctionPipeLine import DisjunctionPipeLine
from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.aoa import AlgebraicPredicate
from ..core.db_restorer import DbRestorer
from ..core.elapsed_time import create_zero_time_profile
from ..core.equi_join import U2EquiJoin
from ..core.filter import Filter
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, NEP_, INEQUALITY, PROJECTION, \
    GROUP_BY, AGGREGATE, ORDER_BY, LIMIT, RESTORE_DB, FILTER, EQUALITY
from mysite.unmasque.src.core.aggregation import Aggregation
from mysite.unmasque.src.core.cs2 import Cs2
from ..util.aoa_utils import get_constants_for
from mysite.unmasque.src.core.from_clause import FromClause
from mysite.unmasque.src.core.groupby_clause import GroupBy
from mysite.unmasque.src.core.limit import Limit
from mysite.unmasque.src.core.nep import NEP
from mysite.unmasque.src.core.orderby_clause import OrderBy
from mysite.unmasque.src.core.projection import Projection
from ..util.utils import get_val_plus_delta, get_format
from mysite.unmasque.src.core.view_minimizer import ViewMinimizer


class ExtractionPipeLine(GenericPipeLine):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Extraction PipeLine")
        self.aoa = None
        self.equi_join = None
        self.key_lists = None
        self.filter_extractor = None
        self.db_restorer = None
        self.global_min_instance_dict = None
        self.mutation_earlyExit = False

    def extract(self, query):
        self.connectionHelper.connectUsingParams()
        '''
        From Clause Extraction
        '''
        self.update_state(FROM_CLAUSE + START)
        fc = FromClause(self.connectionHelper)
        self.update_state(FROM_CLAUSE + RUNNING)
        check = fc.doJob(query)
        self.update_state(FROM_CLAUSE + DONE)
        self.time_profile.update_for_from_clause(fc.local_elapsed_time, fc.app_calls)
        if not check or not fc.done:
            self.logger.error("Some problem while extracting from clause. Aborting!")
            self.info[FROM_CLAUSE] = None
            return None, self.time_profile
        self.info[FROM_CLAUSE] = fc.core_relations

        self.all_sizes = fc.init.all_sizes
        self.key_lists = fc.get_key_lists()

        eq, t = self.after_from_clause_extract(query, self.info[FROM_CLAUSE])
        self.connectionHelper.closeConnection()
        self.time_profile.update(t)
        return eq

    def mutation_pipeline(self, core_relations, query, time_profile, restore_details=None):
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
        cs2 = Cs2(self.connectionHelper, self.db_restorer.last_restored_size, core_relations, self.key_lists)
        self.update_state(SAMPLING + RUNNING)
        check = cs2.doJob(query)
        self.update_state(SAMPLING + DONE)
        time_profile.update_for_cs2(cs2.local_elapsed_time, cs2.app_calls)
        if not check or not cs2.done:
            self.info[SAMPLING] = None
            self.logger.info("Sampling failed!")
        else:
            self.info[SAMPLING] = {'sample': cs2.sample, 'size': cs2.sizes}

        """
        View based Database Minimization
        """
        self.update_state(DB_MINIMIZATION + START)
        vm = ViewMinimizer(self.connectionHelper, core_relations, cs2.sizes, cs2.passed)
        self.update_state(DB_MINIMIZATION + RUNNING)
        check = vm.doJob(query)
        self.update_state(DB_MINIMIZATION + DONE)
        time_profile.update_for_view_minimization(vm.local_elapsed_time, vm.app_calls)
        if not check or not vm.done:
            self.logger.error("Cannot do database minimization. ")
            self.info[DB_MINIMIZATION] = None
            return False, time_profile
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
        time_profile.update_for_where_clause(self.filter_extractor.local_elapsed_time, self.filter_extractor.app_calls)
        if not self.filter_extractor.done:
            self.info[FILTER] = None
            self.logger.error("Some problem in filter extraction!")
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
            self.info[EQUALITY] = None
            self.logger.error("Some problem in Equality predicate extraction!")
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
        self.aoa = AlgebraicPredicate(self.connectionHelper, core_relations, self.equi_join.pending_predicates,
                                      self.equi_join.arithmetic_eq_predicates,
                                      self.equi_join.algebraic_eq_predicates, self.filter_extractor,
                                      self.global_min_instance_dict)
        self.aoa.enabled = False  # ej.pending_predicates are ineq preds as of now
        self.update_state(INEQUALITY + RUNNING)
        check = self.aoa.doJob(query)
        self.update_state(INEQUALITY + DONE)
        time_profile.update_for_where_clause(self.aoa.local_elapsed_time, self.aoa.app_calls)
        self.info[INEQUALITY] = self.aoa.where_clause
        if not check:
            self.info[INEQUALITY] = None
            self.logger.info("Cannot find inequality Predicates.")
        if not self.aoa.done:
            self.info[INEQUALITY] = None
            self.logger.error("Some error while Inequality Predicates extraction. Aborting extraction!")
            return False, time_profile
        return True, time_profile

    def after_from_clause_extract(self, query, core_relations):

        time_profile = create_zero_time_profile()
        q_generator = QueryStringGenerator(self.connectionHelper)

        check, time_profile = self.mutation_pipeline(core_relations, query, time_profile)
        if not check:
            self.logger.error("Some problem in Regular mutation pipeline. Aborting extraction!")
            return None, time_profile

        or_extractor = DisjunctionPipeLine(self)
        check, time_profile, ors = or_extractor.extract((core_relations, query, time_profile))
        if not check:
            self.logger.error("Some problem in disjunction pipeline. Aborting extraction!")
            return None, time_profile

        self.aoa.generate_where_clause(ors)
        self.aoa.post_process_for_generation_pipeline(query)
        self.aoa.pipeline_delivery.doJob()
        return self.aoa.where_clause, time_profile

        delivery = copy.copy(self.aoa.pipeline_delivery)


        '''
        Projection Extraction
        '''
        self.update_state(PROJECTION + START)
        pj = Projection(self.connectionHelper, delivery)

        self.update_state(PROJECTION + RUNNING)
        check = pj.doJob(query)
        self.update_state(PROJECTION + DONE)
        time_profile.update_for_projection(pj.local_elapsed_time, pj.app_calls)
        self.info[PROJECTION] = {'names': pj.projection_names, 'attribs': pj.projected_attribs}
        if not check:
            self.info[PROJECTION] = None
            self.logger.error("Cannot find projected attributes. ")
            return None, time_profile
        if not pj.done:
            self.info[PROJECTION] = None
            self.logger.error("Some error while projection extraction. Aborting extraction!")
            return None, time_profile

        self.update_state(GROUP_BY + START)
        gb = GroupBy(self.connectionHelper, delivery, pj.projected_attribs)
        self.update_state(GROUP_BY + RUNNING)
        check = gb.doJob(query)

        self.update_state(GROUP_BY + DONE)
        time_profile.update_for_group_by(gb.local_elapsed_time, gb.app_calls)
        self.info[GROUP_BY] = gb.group_by_attrib
        if not check:
            self.info[GROUP_BY] = None
            self.logger.info("Cannot find group by attributes. ")

        if not gb.done:
            self.info[GROUP_BY] = None
            self.logger.error("Some error while group by extraction. Aborting extraction!")
            return None, time_profile

        for elt in self.aoa.filter_predicates:
            if elt[1] not in gb.group_by_attrib and elt[1] in pj.projected_attribs and (
                    elt[2] == '=' or elt[2] == 'equal'):
                gb.group_by_attrib.append(elt[1])

        self.update_state(AGGREGATE + START)
        agg = Aggregation(self.connectionHelper, pj.projected_attribs, gb.has_groupby, gb.group_by_attrib,
                          pj.dependencies, pj.solution, pj.param_list, delivery)
        self.update_state(AGGREGATE + RUNNING)
        check = agg.doJob(query)

        self.update_state(AGGREGATE + DONE)
        time_profile.update_for_aggregate(agg.local_elapsed_time, agg.app_calls)
        self.info[AGGREGATE] = agg.global_aggregated_attributes
        if not check:
            self.info[AGGREGATE] = None
            self.logger.info("Cannot find aggregations.")
        if not agg.done:
            self.info[AGGREGATE] = None
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        self.update_state(ORDER_BY + START)
        ob = OrderBy(self.connectionHelper, pj.projected_attribs, pj.projection_names, pj.dependencies,
                     agg.global_aggregated_attributes, delivery)
        self.update_state(ORDER_BY + RUNNING)
        ob.doJob(query)

        self.update_state(ORDER_BY + DONE)
        time_profile.update_for_order_by(ob.local_elapsed_time, ob.app_calls)
        self.info[ORDER_BY] = ob.orderBy_string
        if not ob.has_orderBy:
            self.info[ORDER_BY] = None
            self.logger.info("Cannot find aggregations.")
        if not ob.done:
            self.info[ORDER_BY] = None
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        self.update_state(LIMIT + START)
        lm = Limit(self.connectionHelper, gb.group_by_attrib, delivery)
        self.update_state(LIMIT + RUNNING)
        lm.doJob(query)

        self.update_state(LIMIT + DONE)
        time_profile.update_for_limit(lm.local_elapsed_time, lm.app_calls)
        self.info[LIMIT] = lm.limit
        if lm.limit is None:
            self.info[LIMIT] = None
            self.logger.info("Cannot find limit.")
        if not lm.done:
            self.info[LIMIT] = None
            self.logger.error("Some error while extracting limit. Aborting extraction!")
            return None, time_profile

        eq = q_generator.generate_query_string(core_relations, pj, gb, agg, ob, lm, self.aoa)

        self.logger.debug("extracted query:\n", eq)

        eq = self.extract_NEP(core_relations, {}, eq, q_generator, query, time_profile, delivery)

        # last component in the pipeline should do this
        time_profile.update_for_app(lm.app.method_call_count)

        self.update_state(DONE)
        return eq, time_profile

    def extract_NEP(self, core_relations, sizes, eq, q_generator, query, time_profile, delivery):
        if self.connectionHelper.config.detect_nep:
            self.update_state(NEP_ + START)
            nep = NEP(self.connectionHelper, core_relations, q_generator, delivery, sizes)
            self.update_state(NEP_ + RUNNING)
            check = nep.doJob([query, eq])
            if nep.Q_E:
                eq = nep.Q_E
            time_profile.update_for_nep(nep.local_elapsed_time, nep.app_calls)
            self.update_state(NEP_ + DONE)

            if not check:
                self.logger.info("NEP does not exists.")
        return eq
