import copy

from ...src.core.aggregation import Aggregation
from ...src.core.from_clause import FromClause
from ...src.core.groupby_clause import GroupBy
from ...src.core.limit import Limit
from ...src.core.nep import NEP
from ...src.core.orderby_clause import OrderBy
from ...src.core.projection import Projection
from .DisjunctionPipeLine import DisjunctionPipeLine
from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.elapsed_time import create_zero_time_profile
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, NEP_, PROJECTION, \
    GROUP_BY, AGGREGATE, ORDER_BY, LIMIT


class ExtractionPipeLine(DisjunctionPipeLine):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Extraction PipeLine")

    def process(self, query: str):
        return GenericPipeLine.process(self, query)

    def doJob(self, query, qe=None):
        return GenericPipeLine.doJob(self, query, qe)

    def verify_correctness(self, query, result):
        GenericPipeLine.verify_correctness(self, query, result)

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

        self.core_relations = fc.core_relations

        self.all_sizes = fc.init.all_sizes
        self.key_lists = fc.get_key_lists()

        eq, t = self.after_from_clause_extract(query, self.core_relations)
        self.connectionHelper.closeConnection()
        self.time_profile.update(t)
        return eq

    def after_from_clause_extract(self, query, core_relations):

        time_profile = create_zero_time_profile()
        q_generator = QueryStringGenerator(self.connectionHelper)

        check, time_profile = self.mutation_pipeline(core_relations, query, time_profile)
        if not check:
            self.logger.error("Some problem in Regular mutation pipeline. Aborting extraction!")
            return None, time_profile

        check, time_profile, ors = self.extract_disjunction(self.aoa.filter_predicates,
                                                            core_relations, query, time_profile)
        if not check:
            self.logger.error("Some problem in disjunction pipeline. Aborting extraction!")
            return None, time_profile

        self.aoa.post_process_for_generation_pipeline(query)
        self.aoa.generate_where_clause(ors)

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
