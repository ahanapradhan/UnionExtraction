from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.elapsed_time import create_zero_time_profile
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, EQUI_JOIN, FILTER, \
    NEP_, LIMIT, ORDER_BY, AGGREGATE, GROUP_BY, PROJECTION
from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.equi_join import EquiJoin
from ...refactored.filter import Filter
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

        '''
        Join Graph Extraction
        '''
        self.update_state(EQUI_JOIN + START)
        ej = EquiJoin(self.connectionHelper,
                      key_lists,
                      core_relations,
                      vm.global_min_instance_dict)
        self.update_state(EQUI_JOIN + RUNNING)
        check = ej.doJob(query)
        self.update_state(EQUI_JOIN + DONE)
        time_profile.update_for_where_clause(ej.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find Join Predicates.")
        if not ej.done:
            self.logger.error("Some error while Join Predicate extraction. Aborting extraction!")
            return None, time_profile

        '''
        Filters Extraction
        '''
        self.update_state(FILTER + START)

        fl = Filter(self.connectionHelper,
                    key_lists,
                    core_relations,
                    vm.global_min_instance_dict,
                    ej.global_key_attributes)
        self.update_state(FILTER + RUNNING)
        check = fl.doJob(query)
        self.update_state(FILTER + DONE)
        time_profile.update_for_where_clause(fl.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find Filter Predicates.")
        if not fl.done:
            self.logger.error("Some error while Filter Predicate extraction. Aborting extraction!")
            return None, time_profile

        '''
        Projection Extraction
        '''
        self.update_state(PROJECTION + START)
        pj = Projection(self.connectionHelper, ej.global_attrib_types, core_relations, fl.filter_predicates,
                        ej.global_join_graph, ej.global_all_attribs, vm.global_min_instance_dict,
                        ej.global_key_attributes)
        self.update_state(PROJECTION + RUNNING)
        check = pj.doJob(query)
        self.update_state(PROJECTION + DONE)
        time_profile.update_for_projection(pj.local_elapsed_time)
        if not check:
            self.logger.error("Cannot find projected attributes. ")
            return None, time_profile
        if not pj.done:
            self.logger.error("Some error while projection extraction. Aborting extraction!")
            return None, time_profile
        self.logger.debug("Projection", pj.projected_attribs, pj.param_list, pj.dependencies)

        self.update_state(GROUP_BY + START)

        gb = GroupBy(self.connectionHelper, ej.global_attrib_types, core_relations, fl.filter_predicates,
                     ej.global_all_attribs, ej.global_join_graph, pj.projected_attribs, vm.global_min_instance_dict,
                     ej.global_key_attributes)
        self.update_state(GROUP_BY + RUNNING)
        check = gb.doJob(query)
        self.update_state(GROUP_BY + DONE)
        time_profile.update_for_group_by(gb.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find group by attributes. ")

        if not gb.done:
            self.logger.error("Some error while group by extraction. Aborting extraction!")
            return None, time_profile

        for elt in fl.filter_predicates:
            if elt[1] not in gb.group_by_attrib and elt[1] in pj.projected_attribs and (
                    elt[2] == '=' or elt[2] == 'equal'):
                gb.group_by_attrib.append(elt[1])

        self.update_state(AGGREGATE + START)
        agg = Aggregation(self.connectionHelper, ej.global_key_attributes, ej.global_attrib_types, core_relations,
                          fl.filter_predicates, ej.global_all_attribs, ej.global_join_graph, pj.projected_attribs,
                          gb.has_groupby, gb.group_by_attrib, pj.dependencies, pj.solution, pj.param_list,
                          vm.global_min_instance_dict)
        self.update_state(AGGREGATE + RUNNING)
        check = agg.doJob(query)
        self.update_state(AGGREGATE + DONE)
        time_profile.update_for_aggregate(agg.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find aggregations.")
        if not agg.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile
        self.logger.debug("Aggregation", agg.global_aggregated_attributes)

        self.update_state(ORDER_BY + START)
        ob = OrderBy(self.connectionHelper, ej.global_key_attributes, ej.global_attrib_types, core_relations,
                     fl.filter_predicates, ej.global_all_attribs, ej.global_join_graph, pj.projected_attribs,
                     pj.projection_names, pj.dependencies, agg.global_aggregated_attributes,
                     vm.global_min_instance_dict)
        self.update_state(ORDER_BY + RUNNING)
        ob.doJob(query)
        self.update_state(ORDER_BY + DONE)
        time_profile.update_for_order_by(ob.local_elapsed_time)
        if not ob.has_orderBy:
            self.logger.info("Cannot find aggregations.")
        if not ob.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        self.update_state(LIMIT + START)
        lm = Limit(self.connectionHelper, ej.global_attrib_types, ej.global_key_attributes, core_relations,
                   fl.filter_predicates, ej.global_all_attribs, gb.group_by_attrib, vm.global_min_instance_dict)
        self.update_state(LIMIT + RUNNING)
        lm.doJob(query)
        self.update_state(LIMIT + DONE)
        time_profile.update_for_limit(lm.local_elapsed_time)
        if lm.limit is None:
            self.logger.info("Cannot find limit.")
        if not lm.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        q_generator = QueryStringGenerator(self.connectionHelper)
        eq = q_generator.generate_query_string(core_relations, ej, fl, pj, gb, agg, ob, lm)
        self.logger.debug("extracted query:\n", eq)

        eq = self.extract_NEP(core_relations, cs2, ej, eq, fl, q_generator, query, time_profile, vm)

        # last component in the pipeline should do this
        time_profile.update_for_app(lm.app.method_call_count)

        self.update_state(DONE)
        return eq, time_profile

    def extract_NEP(self, core_relations, cs2, ej, eq, fl, q_generator, query, time_profile, vm):
        if self.connectionHelper.config.detect_nep:
            self.update_state(NEP_ + START)
            nep = NEP(self.connectionHelper, core_relations, cs2.sizes, self.global_pk_dict, ej.global_all_attribs,
                      ej.global_attrib_types, fl.filter_predicates, ej.global_key_attributes, q_generator,
                      vm.global_min_instance_dict)
            self.update_state(NEP_ + RUNNING)
            check = nep.doJob([query, eq])
            if nep.Q_E:
                eq = nep.Q_E
            time_profile.update_for_nep(nep.local_elapsed_time)
            self.update_state(NEP_ + DONE)

            if not check:
                self.logger.info("NEP does not exists.")
        return eq
