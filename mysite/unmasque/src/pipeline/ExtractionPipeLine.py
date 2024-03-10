import copy

from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.aoa import AlgebraicPredicate
from ..core.elapsed_time import create_zero_time_profile
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, NEP_, LIMIT, ORDER_BY, \
    AGGREGATE, GROUP_BY, PROJECTION, AOA
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
        self.time_profile.update_for_from_clause(fc.local_elapsed_time)
        if not check or not fc.done:
            self.logger.error("Some problem while extracting from clause. Aborting!")
            return None, self.time_profile

        self.all_relations = fc.all_relations
        self.global_pk_dict = fc.init.global_pk_dict

        eq, t = self.after_from_clause_extract(query, self.all_relations,
                                               fc.core_relations,
                                               fc.get_key_lists())
        self.connectionHelper.closeConnection()
        self.time_profile.update(t)
        return eq

    def after_from_clause_extract(self,
                                  query, all_relations,
                                  core_relations,
                                  key_lists):  # get core_relations, key_lists from from clause

        time_profile = create_zero_time_profile()

        '''
        Correlated Sampling
        '''
        self.update_state(SAMPLING + START)
        cs2 = Cs2(self.connectionHelper, all_relations, core_relations, key_lists)
        self.update_state(SAMPLING + RUNNING)
        check = cs2.doJob(query)
        self.update_state(SAMPLING + DONE)
        time_profile.update_for_cs2(cs2.local_elapsed_time)
        if not check or not cs2.done:
            self.logger.info("Sampling failed!")

        self.update_state(DB_MINIMIZATION + START)
        vm = ViewMinimizer(self.connectionHelper, core_relations, cs2.sizes, cs2.passed)
        self.update_state(DB_MINIMIZATION + RUNNING)
        check = vm.doJob(query)
        self.update_state(DB_MINIMIZATION + DONE)
        time_profile.update_for_view_minimization(vm.local_elapsed_time)
        if not check:
            self.logger.error("Cannot do database minimization. ")
            return None, time_profile
        if not vm.done:
            self.logger.error("Some problem while view minimization. Aborting extraction!")
            return None, time_profile
        print(vm.global_min_instance_dict)

        '''
        AOA Extraction
        '''
        self.update_state(AOA + START)
        self.global_min_instance_dict = copy.deepcopy(vm.global_min_instance_dict)
        aoa = AlgebraicPredicate(self.connectionHelper, core_relations, self.global_min_instance_dict)
        self.update_state(AOA + RUNNING)
        check = aoa.doJob(query)
        # print(aoa.pipeline_delivery.global_min_instance_dict)
        self.update_state(AOA + DONE)
        time_profile.update_for_where_clause(aoa.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find Filter Predicates.")
        if not aoa.done:
            self.logger.error("Some error while Filter Predicate extraction. Aborting extraction!")
            return None, time_profile

        delivery = aoa.pipeline_delivery

        '''
        Projection Extraction
        '''
        self.update_state(PROJECTION + START)
        pj = Projection(self.connectionHelper, delivery)

        self.update_state(PROJECTION + RUNNING)
        check = pj.doJob(query)
        print(delivery.global_min_instance_dict)
        self.update_state(PROJECTION + DONE)
        time_profile.update_for_projection(pj.local_elapsed_time)
        if not check:
            self.logger.error("Cannot find projected attributes. ")
            return None, time_profile
        if not pj.done:
            self.logger.error("Some error while projection extraction. Aborting extraction!")
            return None, time_profile

        self.update_state(GROUP_BY + START)
        gb = GroupBy(self.connectionHelper, delivery, pj.projected_attribs)
        self.update_state(GROUP_BY + RUNNING)
        check = gb.doJob(query)
        print(delivery.global_min_instance_dict)

        self.update_state(GROUP_BY + DONE)
        time_profile.update_for_group_by(gb.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find group by attributes. ")

        if not gb.done:
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
        print(delivery.global_min_instance_dict)

        self.update_state(AGGREGATE + DONE)
        time_profile.update_for_aggregate(agg.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find aggregations.")
        if not agg.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        self.update_state(ORDER_BY + START)
        ob = OrderBy(self.connectionHelper, pj.projected_attribs, pj.projection_names, pj.dependencies,
                     agg.global_aggregated_attributes, delivery)
        self.update_state(ORDER_BY + RUNNING)
        ob.doJob(query)
        print(delivery.global_min_instance_dict)

        self.update_state(ORDER_BY + DONE)
        time_profile.update_for_order_by(ob.local_elapsed_time)
        if not ob.has_orderBy:
            self.logger.info("Cannot find order by.")
        if not ob.done:
            self.logger.error("Some error while extracting order by. Aborting extraction!")
            return None, time_profile

        self.update_state(LIMIT + START)
        lm = Limit(self.connectionHelper, gb.group_by_attrib, delivery)
        self.update_state(LIMIT + RUNNING)
        lm.doJob(query)
        print(delivery.global_min_instance_dict)

        self.update_state(LIMIT + DONE)
        time_profile.update_for_limit(lm.local_elapsed_time)
        if lm.limit is None:
            self.logger.info("Cannot find limit.")
        if not lm.done:
            self.logger.error("Some error while extracting limit. Aborting extraction!")
            return None, time_profile

        q_generator = QueryStringGenerator(self.connectionHelper)
        eq = q_generator.generate_query_string(core_relations, pj, gb, agg, ob, lm, aoa)
        self.logger.debug("extracted query:\n", eq)

        eq = self.extract_NEP(core_relations, cs2.sizes, eq, q_generator, query, time_profile, delivery)

        # last component in the pipeline should do this
        time_profile.update_for_app(lm.app.method_call_count)

        self.update_state(DONE)
        return eq, time_profile

    def extract_NEP(self, core_relations, sizes, eq, q_generator, query, time_profile, delivery):
        if self.connectionHelper.config.detect_nep:
            self.update_state(NEP_ + START)
            nep = NEP(self.connectionHelper, core_relations, sizes, q_generator, delivery)
            self.update_state(NEP_ + RUNNING)
            check = nep.doJob([query, eq])
            if nep.Q_E:
                eq = nep.Q_E
            time_profile.update_for_nep(nep.local_elapsed_time)
            self.update_state(NEP_ + DONE)

            if not check:
                self.logger.info("NEP does not exists.")
        return eq
