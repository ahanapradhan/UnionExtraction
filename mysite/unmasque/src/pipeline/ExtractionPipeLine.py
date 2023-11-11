import copy

from .abstract.generic_pipeline import GenericPipeLine
from ..core.QueryStringGenerator import QueryStringGenerator
from ..core.elapsed_time import create_zero_time_profile
from ..core.multiple_equi_joins import MultipleEquiJoin
from ..core.multiple_filters import MultipleFilter
from ..core.multiple_projections import MultipleProjection
from ..core.n_minimizer import NMinimizer
from mysite.unmasque.src.core.abstract.spj_QueryStringGenerator import SPJQueryStringGenerator
from ..util.constants import FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, EQUI_JOIN, FILTER, \
    NEP_, LIMIT, ORDER_BY, AGGREGATE, GROUP_BY, PROJECTION
from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.from_clause import FromClause
from ...refactored.groupby_clause import GroupBy
from ...refactored.limit import Limit
from ...refactored.nep import NEP
from ...refactored.orderby_clause import OrderBy
from ...refactored.projection import Projection


class ExtractionPipeLine(GenericPipeLine):
    FROM_IDX = 0
    CS2_IDX = 0
    DB_MIN_IDX = 0
    JOIN_IDX = 0
    FILTER_IDX = 0
    PROJECTION_IDX = 0
    GB_IDX = 0
    AGG_IDX = 0
    OB_IDX = 0
    LIMIT_IDX = 0

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
        check = fc.doJob(query, "rename")
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

    def run_db_minimization(self, all_relations,
                            core_relations,
                            key_lists,
                            query,
                            time_profile):
        next_modules = []
        can_progress = True

        """
        Correlated Sampling
        """
        self.update_state(SAMPLING + START)
        cs2 = Cs2(self.connectionHelper, all_relations, core_relations, key_lists)
        self.update_state(SAMPLING + RUNNING)
        check = cs2.doJob(query)
        self.update_state(SAMPLING + DONE)
        time_profile.update_for_cs2(cs2.local_elapsed_time)
        if not check or not cs2.done:
            self.logger.info("Sampling failed!")

        next_modules.append(cs2)
        self.CS2_IDX = len(next_modules) - 1

        self.update_state(DB_MINIMIZATION + START)
        vm = NMinimizer(self.connectionHelper, core_relations, cs2.sizes)
        self.update_state(DB_MINIMIZATION + RUNNING)
        check = vm.doJob(query)
        self.update_state(DB_MINIMIZATION + DONE)
        time_profile.update_for_view_minimization(vm.local_elapsed_time)
        if not check:
            self.logger.error("Cannot do database minimization. ")
            can_progress = False
        if not vm.done:
            self.logger.error("Some problem while view minimization. Aborting extraction!")
            can_progress = False

        next_modules.append(vm)
        self.DB_MIN_IDX = len(next_modules) - 1
        return can_progress, next_modules

    def run_mutation_pipeline(self, query, key_lists, core_relations, global_min_instance_dict, time_profile):
        next_modules = []
        can_progress = True
        '''
        Join Graph Extraction
        '''
        self.update_state(EQUI_JOIN + START)
        ej = MultipleEquiJoin(self.connectionHelper,
                              key_lists,
                              core_relations,
                              global_min_instance_dict)
        self.update_state(EQUI_JOIN + RUNNING)
        check = ej.doJob(query)
        self.update_state(EQUI_JOIN + DONE)
        time_profile.update_for_where_clause(ej.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find Join Predicates.")
        if not ej.done:
            self.logger.error("Some error while Join Predicate extraction. Aborting extraction!")
            can_progress = False

        if not can_progress:
            return False

        next_modules.append(ej)
        self.JOIN_IDX = len(next_modules) - 1

        '''
        Filters Extraction
        '''
        self.update_state(FILTER + START)
        fl = MultipleFilter(self.connectionHelper,
                            key_lists,
                            ej.fromData, ej.joinData,
                            ej.d_min_DictData)
        self.update_state(FILTER + RUNNING)
        check = fl.doJob(query)
        self.update_state(FILTER + DONE)
        time_profile.update_for_where_clause(fl.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find Filter Predicates.")
        if not fl.done:
            self.logger.error("Some error while Filter Predicate extraction. Aborting extraction!")
            can_progress = False

        if not can_progress:
            return False

        next_modules.append(fl)
        self.FILTER_IDX = len(next_modules) - 1

        '''
        Projection Extraction
        '''
        self.update_state(PROJECTION + START)

        if ej.intersection:
            self.logger.info("It is an intersection Query")
            pj = MultipleProjection(self.connectionHelper, key_lists,
                                    ej.fromData, ej.joinData, fl.filterData,
                                    ej.d_min_DictData)
        else:
            pj = Projection(self.connectionHelper, ej.join_extractor.global_attrib_types, core_relations,
                            fl.filterData[0].filter_predicates,
                            ej.join_extractor.global_join_graph, ej.join_extractor.global_all_attribs,
                            ej.join_extractor.global_min_instance_dict,
                            ej.join_extractor.global_key_attributes)
        can_progress = self.extract_projection(can_progress, next_modules, pj, query, time_profile)
        return can_progress, next_modules

    def extract_projection(self, can_progress, next_modules, pj, query, time_profile):
        self.update_state(PROJECTION + RUNNING)
        check = pj.doJob(query)
        self.update_state(PROJECTION + DONE)
        time_profile.update_for_projection(pj.local_elapsed_time)
        if not check:
            self.logger.error("Cannot find projected attributes. ")
            can_progress = False
        if not pj.done:
            self.logger.error("Some error while projection extraction. Aborting extraction!")
            can_progress = False
        next_modules.append(pj)
        self.PROJECTION_IDX = len(next_modules) - 1
        return can_progress

    def run_generation_pipeline(self, query, core_relations,
                                modules, global_min_instance_dict,
                                time_profile):
        ej, fl, pj = modules[self.JOIN_IDX], modules[self.FILTER_IDX], \
            modules[self.PROJECTION_IDX]
        next_modules = []
        can_progress = True

        self.update_state(GROUP_BY + START)
        gb = GroupBy(self.connectionHelper, ej.global_attrib_types, core_relations, fl.filter_predicates,
                     ej.global_all_attribs, ej.global_join_graph, pj.projected_attribs, global_min_instance_dict,
                     ej.global_key_attributes)
        self.update_state(GROUP_BY + RUNNING)
        check = gb.doJob(query)
        self.update_state(GROUP_BY + DONE)
        time_profile.update_for_group_by(gb.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find group by attributes. ")

        if not gb.done:
            self.logger.error("Some error while group by extraction. Aborting extraction!")
            can_progress = False

        if not can_progress:
            return False

        next_modules.append(gb)
        self.GB_IDX = len(next_modules) - 1

        self.update_state(AGGREGATE + START)
        agg = Aggregation(self.connectionHelper, ej.global_key_attributes, ej.global_attrib_types, core_relations,
                          fl.filter_predicates, ej.global_all_attribs, ej.global_join_graph, pj.projected_attribs,
                          gb.has_groupby, gb.group_by_attrib, pj.dependencies, pj.solution, pj.param_list,
                          global_min_instance_dict)
        self.update_state(AGGREGATE + RUNNING)
        check = agg.doJob(query)
        self.update_state(AGGREGATE + DONE)
        time_profile.update_for_aggregate(agg.local_elapsed_time)
        if not check:
            self.logger.info("Cannot find aggregations.")
        if not agg.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            can_progress = False
        self.logger.debug("Aggregation", agg.global_aggregated_attributes)

        if not can_progress:
            return False

        next_modules.append(agg)
        self.AGG_IDX = len(next_modules) - 1

        self.update_state(ORDER_BY + START)
        ob = OrderBy(self.connectionHelper, ej.global_key_attributes, ej.global_attrib_types, core_relations,
                     fl.filter_predicates, ej.global_all_attribs, ej.global_join_graph, pj.projected_attribs,
                     pj.projection_names, pj.dependencies, agg.global_aggregated_attributes,
                     global_min_instance_dict)
        self.update_state(ORDER_BY + RUNNING)
        ob.doJob(query)
        self.update_state(ORDER_BY + DONE)
        time_profile.update_for_order_by(ob.local_elapsed_time)
        if not ob.has_orderBy:
            self.logger.info("Cannot find aggregations.")
        if not ob.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            can_progress = False

        if not can_progress:
            return False

        next_modules.append(ob)
        self.OB_IDX = len(next_modules) - 1

        self.update_state(LIMIT + START)
        lm = Limit(self.connectionHelper, ej.global_attrib_types, ej.global_key_attributes, core_relations,
                   fl.filter_predicates, ej.global_all_attribs, gb.group_by_attrib, global_min_instance_dict)
        self.update_state(LIMIT + RUNNING)
        lm.doJob(query)
        self.update_state(LIMIT + DONE)
        time_profile.update_for_limit(lm.local_elapsed_time)
        if lm.limit is None:
            self.logger.info("Cannot find limit.")
        if not lm.done:
            self.logger.error("Some error while extrating aggregations. Aborting extraction!")
            can_progress = False

        next_modules.append(lm)
        self.LIMIT_IDX = len(next_modules) - 1

        return can_progress, next_modules

    def after_from_clause_extract(self,
                                  query, all_relations,
                                  core_relations,
                                  key_lists):  # get core_relations, key_lists from from clause

        time_profile = create_zero_time_profile()

        can_progress, minimizer_modules = self.run_db_minimization(all_relations,
                                                                   core_relations,
                                                                   key_lists,
                                                                   query,
                                                                   time_profile)
        if not can_progress:
            return None, time_profile

        global_min_instance_dict = minimizer_modules[self.DB_MIN_IDX].global_min_instance_dict

        can_progress, mutation_modules = self.run_mutation_pipeline(query,
                                                                    key_lists,
                                                                    core_relations,
                                                                    global_min_instance_dict,
                                                                    time_profile)
        if not can_progress:
            return None, time_profile

        if mutation_modules[self.JOIN_IDX].intersection:
            if can_progress:
                eq = self.generate_intersection_query_string(mutation_modules)
            else:
                eq = None
            return eq, time_profile

        useful_mutation_modules = self.prepare_modules_for_singleQuery(mutation_modules)

        can_progress, generation_modules = self.run_generation_pipeline(query,
                                                                        core_relations,
                                                                        useful_mutation_modules,
                                                                        global_min_instance_dict,
                                                                        time_profile)
        if not can_progress:
            return None, time_profile

        q_generator = QueryStringGenerator(self.connectionHelper)

        useful_modules = useful_mutation_modules + generation_modules
        useful_modules.insert(0, core_relations)
        eq = q_generator.generate_query_string(useful_modules)
        self.logger.debug("extracted query:\n", eq)

        eq = self.extract_NEP(core_relations, minimizer_modules[self.CS2_IDX].sizes,
                              useful_mutation_modules[self.JOIN_IDX], eq,
                              useful_mutation_modules[self.FILTER_IDX].filter_predicates,
                              q_generator, query,
                              time_profile, global_min_instance_dict)

        # last component in the pipeline should do this
        time_profile.update_for_app(generation_modules[self.LIMIT_IDX].app.method_call_count)

        self.update_state(DONE)
        return eq, time_profile

    def prepare_modules_for_singleQuery(self, mutation_modules):
        joins = mutation_modules[self.JOIN_IDX].join_extractor
        filters = mutation_modules[self.FILTER_IDX].filterData[0]
        useful_mutation_modules = [joins, filters, mutation_modules[self.PROJECTION_IDX]]
        return useful_mutation_modules

    def generate_intersection_query_string(self, mutation_modules):
        subq_strings = []
        subquery_generator = SPJQueryStringGenerator(self.connectionHelper)
        froms = mutation_modules[self.JOIN_IDX].fromData
        joins = mutation_modules[self.JOIN_IDX].joinData
        filters = mutation_modules[self.FILTER_IDX].filterData
        projections = mutation_modules[self.PROJECTION_IDX].projectionData
        for i in range(len(froms)):
            subq_modules = [froms[i].core_relations, joins[i], filters[i], projections]
            subq_str = "(" + subquery_generator.generate_query_string(subq_modules) + ")"
            subq_str = subq_str.replace(";", "")
            subq_strings.append(subq_str)
        eq = "\n INTERSECT \n".join(subq_strings)
        eq = eq + ";"
        return eq

    def extract_NEP(self, core_relations, sizes, ej, eq, filter_predicates,
                    q_generator, query, time_profile,
                    global_min_instance_dict):

        if self.connectionHelper.config.detect_nep:
            self.update_state(NEP_ + START)
            nep = NEP(self.connectionHelper, core_relations, sizes, self.global_pk_dict, ej.global_all_attribs,
                      ej.global_attrib_types, filter_predicates, ej.global_key_attributes, q_generator,
                      global_min_instance_dict)
            self.update_state(NEP_ + RUNNING)
            check = nep.doJob([query, eq])
            eq = nep.Q_E
            time_profile.update_for_nep(nep.local_elapsed_time)
            self.update_state(NEP_ + DONE)

            if not check:
                self.logger.info("NEP does not exists.")
        return eq
