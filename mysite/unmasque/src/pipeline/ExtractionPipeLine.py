import copy

from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.aoa import AlgebraicPredicate
from ..core.elapsed_time import create_zero_time_profile
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, NEP_, AOA, PROJECTION, \
    GROUP_BY, AGGREGATE, ORDER_BY, LIMIT
from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.from_clause import FromClause
from ...refactored.groupby_clause import GroupBy
from ...refactored.limit import Limit
from ...refactored.nep import NEP
from ...refactored.orderby_clause import OrderBy
from ...refactored.projection import Projection
from ...refactored.view_minimizer import ViewMinimizer


class ExtractionPipeLine(GenericPipeLine):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Extraction PipeLine")
        self.global_pk_dict = {}
        self.global_min_instance_dict = None

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

        self.all_relations = fc.all_relations
        self.global_pk_dict = fc.init.global_pk_dict

        self.info[FROM_CLAUSE] = fc.core_relations

        eq, t = self.after_from_clause_extract(query, fc.core_relations, fc.get_key_lists())
        self.connectionHelper.closeConnection()
        self.time_profile.update(t)
        return eq

    def nullify_predicates(self, preds, get_datatype):
        for pred in preds:
            if not len(pred):
                return False
            tab, attrib, op = pred[0], pred[1], pred[2]
            lb, ub = pred[3], pred[4]
            if op in ['equal', '=']:
                datatype = get_datatype((tab, attrib))
                val = f"\'{lb}\'" if datatype in ['str', 'date'] else lb
                mutatation_query = f"UPDATE {tab} SET {attrib} = NULL WHERE {attrib} = {val};"
            else:
                mutatation_query = f"UPDATE {tab} SET {attrib} = NULL WHERE {attrib} >= {lb} and {attrib} <= {ub};"
            self.connectionHelper.execute_sql([mutatation_query], self.logger)
        return True

    def mutation_pipeline(self, core_relations, key_lists, query, time_profile, aoa=None):

        """
        Correlated Sampling
        """
        self.update_state(SAMPLING + START)
        cs2 = Cs2(self.connectionHelper, self.all_relations, core_relations, key_lists)
        self.update_state(SAMPLING + RUNNING)
        check = cs2.doJob(query)
        self.update_state(SAMPLING + DONE)
        time_profile.update_for_cs2(cs2.local_elapsed_time, cs2.app_calls)
        self.info[SAMPLING] = {'sample': cs2.sample, 'size': cs2.sizes}
        if not check or not cs2.done:
            self.info[SAMPLING] = None
            self.logger.info("Sampling failed!")

        """
        View based Database Minimization
        """
        self.update_state(DB_MINIMIZATION + START)
        vm = ViewMinimizer(self.connectionHelper, core_relations, cs2.sizes, cs2.passed)
        vm.set_all_relations(self.all_relations)
        self.update_state(DB_MINIMIZATION + RUNNING)
        check = vm.doJob(query)
        self.update_state(DB_MINIMIZATION + DONE)
        time_profile.update_for_view_minimization(vm.local_elapsed_time, vm.app_calls)
        self.info[DB_MINIMIZATION] = vm.global_min_instance_dict
        if not check:
            self.logger.error("Cannot do database minimization. ")
            self.info[DB_MINIMIZATION] = None
            return aoa, time_profile
        if not vm.done:
            self.info[DB_MINIMIZATION] = None
            self.logger.error("Some problem while view minimization. Aborting extraction!")
            return aoa, time_profile

        '''
        AOA Extraction
        '''
        self.update_state(AOA + START)
        self.global_min_instance_dict = copy.deepcopy(vm.global_min_instance_dict)
        if aoa is None:
            aoa = AlgebraicPredicate(self.connectionHelper, core_relations, self.global_min_instance_dict)
        else:
            aoa.set_global_min_instance_dict(self.global_min_instance_dict)
            aoa.reset()
        self.update_state(AOA + RUNNING)
        check = aoa.doJob(query)
        self.update_state(AOA + DONE)
        time_profile.update_for_where_clause(aoa.local_elapsed_time, aoa.app_calls)
        self.info[AOA] = aoa.where_clause

        if not check:
            self.info[AOA] = None
            self.logger.info("Cannot find Algebraic Predicates.")
        if not aoa.done:
            self.info[AOA] = None
            self.logger.error("Some error while Where Clause extraction. Aborting extraction!")
        return aoa, time_profile

    def after_from_clause_extract(self, query, core_relations, key_lists):

        time_profile = create_zero_time_profile()
        q_generator = QueryStringGenerator(self.connectionHelper)

        aoa, time_profile = self.mutation_pipeline(core_relations, key_lists, query, time_profile, None)
        if self.info[DB_MINIMIZATION] is None:
            return None, time_profile
        if not aoa.done:
            return None, time_profile
        delivery = aoa.pipeline_delivery

        aoa, time_profile, ors = self.extract_disjunction(aoa, core_relations, key_lists, query, time_profile)
        aoa.generate_where_clause(ors)
        if self.connectionHelper.config.detect_or:
            return aoa.where_clause, time_profile

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

        for elt in aoa.filter_predicates:
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

        eq = q_generator.generate_query_string(core_relations, pj, gb, agg, ob, lm, aoa)

        self.logger.debug("extracted query:\n", eq)

        eq = self.extract_NEP(core_relations, {}, eq, q_generator, query, time_profile, delivery)

        # last component in the pipeline should do this
        time_profile.update_for_app(lm.app.method_call_count)

        self.update_state(DONE)
        return eq, time_profile

    def extract_disjunction(self, aoa, core_relations, key_lists, query, time_profile):  # for once
        old_preds = copy.deepcopy(aoa.arithmetic_eq_predicates)
        all_preds = [old_preds]
        max_or_count = len(old_preds)
        while True:
            if self.connectionHelper.config.detect_or:
                or_predicates = []
                aoa.equi_join_enabled = False
                for i in range(max_or_count):
                    in_candidates = [copy.deepcopy(em[i]) for em in all_preds]
                    self.logger.debug("Checking OR predicate of ", in_candidates)
                    for tab in core_relations:
                        aoa.app.sanitize_one_table(tab)
                    nullified = self.nullify_predicates(in_candidates, aoa.get_datatype)
                    if not nullified:
                        or_predicates.append(())
                        break
                    aoa, time_profile = self.mutation_pipeline(core_relations, key_lists, query, time_profile, aoa)
                    if self.info[DB_MINIMIZATION] is None or \
                            self.info[AOA] is None or not aoa.arithmetic_eq_predicates:
                        or_predicates.append(())
                    else:
                        or_predicates.append(aoa.arithmetic_eq_predicates[i])
                    self.logger.debug("new or predicates...", all_preds, or_predicates)
                if all(element == () for element in or_predicates):
                    break
                all_preds.append(or_predicates)
            else:
                break
        all_ors = list(zip(*all_preds))
        return aoa, time_profile, all_ors

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
