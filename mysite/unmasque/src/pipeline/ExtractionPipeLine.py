from time import sleep

from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.elapsed_time import create_zero_time_profile
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, EQUI_JOIN, FILTER, \
    PROJECTION, GROUP_BY, AGGREGATE, ORDER_BY, LIMIT, RESULT_COMPARE
from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.equi_join import EquiJoin
from ...refactored.filter import Filter
from ...refactored.from_clause import FromClause
from ...refactored.groupby_clause import GroupBy
from ...refactored.limit import Limit
from ...refactored.orderby_clause import OrderBy
from ...refactored.projection import Projection
from ...refactored.result_comparator import ResultComparator
from ...refactored.view_minimizer import ViewMinimizer


class ExtractionPipeLine(GenericPipeLine):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Extraction PipeLine")

    def extract(self, query):
        self.connectionHelper.connectUsingParams()
        '''
        From Clause Extraction
        '''
        self.update_state(FROM_CLAUSE + START)
        fc = FromClause(self.connectionHelper)
        self.update_state(FROM_CLAUSE + RUNNING)
        check = fc.doJob(query, "rename")
        self.update_state(FROM_CLAUSE + DONE)
        self.time_profile.update_for_from_clause(fc.local_elapsed_time)
        if not check or not fc.done:
            print("Some problem while extracting from clause. Aborting!")
            return None, self.time_profile

        sleep(1)

        eq, t = self.after_from_clause_extract(query,
                                               fc.all_relations,
                                               fc.core_relations,
                                               fc.get_key_lists())
        self.connectionHelper.closeConnection()
        self.time_profile.update(t)
        return eq

    def after_from_clause_extract(self,
                                  query,
                                  all_relations,
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
            print("Sampling failed!")

        sleep(1)

        self.update_state(DB_MINIMIZATION + START)
        vm = ViewMinimizer(self.connectionHelper, core_relations, cs2.sizes, cs2.passed)
        self.update_state(DB_MINIMIZATION + RUNNING)
        check = vm.doJob(query)
        self.update_state(DB_MINIMIZATION + DONE)
        time_profile.update_for_view_minimization(vm.local_elapsed_time)
        if not check:
            print("Cannot do database minimization. ")
            return None, time_profile
        if not vm.done:
            print("Some problem while view minimization. Aborting extraction!")
            return None, time_profile

        sleep(1)

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
            print("Cannot find Join Predicates.")
        if not ej.done:
            print("Some error while Join Predicate extraction. Aborting extraction!")
            return None, time_profile

        sleep(1)

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
            print("Cannot find Filter Predicates.")
        if not fl.done:
            print("Some error while Filter Predicate extraction. Aborting extraction!")
            return None, time_profile

        sleep(1)

        '''
        Projection Extraction
        '''
        self.update_state(PROJECTION + START)
        pj = Projection(self.connectionHelper,
                        ej.global_attrib_types,
                        core_relations,
                        fl.filter_predicates,
                        ej.global_join_graph,
                        ej.global_all_attribs)
        self.update_state(PROJECTION + RUNNING)
        check = pj.doJob(query)
        self.update_state(PROJECTION + DONE)
        time_profile.update_for_projection(pj.local_elapsed_time)
        if not check:
            print("Cannot find projected attributes. ")
            return None, time_profile
        if not pj.done:
            print("Some error while projection extraction. Aborting extraction!")
            return None, time_profile

        sleep(1)

        self.update_state(GROUP_BY + START)
        gb = GroupBy(self.connectionHelper,
                     ej.global_attrib_types,
                     core_relations,
                     fl.filter_predicates,
                     ej.global_all_attribs,
                     ej.global_join_graph,
                     pj.projected_attribs)
        self.update_state(GROUP_BY + RUNNING)
        check = gb.doJob(query)
        self.update_state(GROUP_BY + DONE)
        time_profile.update_for_group_by(gb.local_elapsed_time)
        if not check:
            print("Cannot find group by attributes. ")

        if not gb.done:
            print("Some error while group by extraction. Aborting extraction!")
            return None, time_profile

        sleep(1)

        self.update_state(AGGREGATE + START)
        agg = Aggregation(self.connectionHelper,
                          ej.global_key_attributes,
                          ej.global_attrib_types,
                          core_relations,
                          fl.filter_predicates,
                          ej.global_all_attribs,
                          ej.global_join_graph,
                          pj.projected_attribs,
                          gb.has_groupby,
                          gb.group_by_attrib,
                          pj.dependencies,
                          pj.solution,
                          pj.param_list)
        self.update_state(AGGREGATE + RUNNING)
        check = agg.doJob(query)
        self.update_state(AGGREGATE + DONE)
        time_profile.update_for_aggregate(agg.local_elapsed_time)
        if not check:
            print("Cannot find aggregations.")
        if not agg.done:
            print("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        sleep(1)

        self.update_state(ORDER_BY + START)
        ob = OrderBy(self.connectionHelper,
                     ej.global_key_attributes,
                     ej.global_attrib_types,
                     core_relations,
                     fl.filter_predicates,
                     ej.global_all_attribs,
                     ej.global_join_graph,
                     pj.projected_attribs,
                     pj.projection_names,
                     agg.global_aggregated_attributes)
        self.update_state(ORDER_BY + RUNNING)
        ob.doJob(query)
        self.update_state(ORDER_BY + DONE)
        time_profile.update_for_order_by(ob.local_elapsed_time)
        if not ob.has_orderBy:
            print("Cannot find aggregations.")
        if not ob.done:
            print("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        sleep(1)

        self.update_state(LIMIT + START)
        lm = Limit(self.connectionHelper,
                   ej.global_attrib_types,
                   ej.global_key_attributes,
                   core_relations,
                   fl.filter_predicates,
                   ej.global_all_attribs,
                   gb.group_by_attrib)
        self.update_state(LIMIT + RUNNING)
        lm.doJob(query)
        self.update_state(LIMIT + DONE)
        time_profile.update_for_limit(lm.local_elapsed_time)
        if lm.limit is None:
            print("Cannot find limit.")
        if not lm.done:
            print("Some error while extrating aggregations. Aborting extraction!")
            return None, time_profile

        sleep(1)

        q_generator = QueryStringGenerator(self.connectionHelper)
        eq = q_generator.generate_query_string(core_relations, ej, fl, pj, gb, agg, ob, lm)
        print("extracted query:\n", eq)

        self.update_state(RESULT_COMPARE + START)
        rc_hash = ResultComparator(self.connectionHelper, True)
        self.update_state(RESULT_COMPARE + RUNNING)
        check = rc_hash.doJob(query, eq)
        time_profile.update_for_result_comparator(rc_hash.local_elapsed_time)
        self.update_state(RESULT_COMPARE + DONE)
        if not check:
            print(query)
            print("========Exracted Query seems different!========")
            print(eq)
            return None, time_profile

        self.update_state(DONE)

        # last component in the pipeline should do this
        time_profile.update_for_app(lm.app.method_call_count)

        return eq, time_profile
