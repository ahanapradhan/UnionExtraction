from .QueryStringGenerator import QueryStringGenerator
from .elapsed_time import create_zero_time_profile
from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.groupby_clause import GroupBy
from ...refactored.limit import Limit
from ...refactored.orderby_clause import OrderBy
from ...refactored.projection import Projection
from ...refactored.view_minimizer import ViewMinimizer
from ...refactored.where_clause import WhereClause


def extract(connectionHelper,
            query,
            all_relations,
            core_relations,
            key_lists,
            global_pk_dict):  # get core_relations, key_lists from from clause

    time_profile = create_zero_time_profile()

    cs2 = Cs2(connectionHelper, all_relations, core_relations, key_lists)
    cs2.doJob(query)
    time_profile.update_for_cs2(cs2.local_elapsed_time)

    vm = ViewMinimizer(connectionHelper, core_relations, cs2.sizes, cs2.passed)
    check = vm.doJob(query)
    time_profile.update_for_view_minimization(vm.local_elapsed_time)
    if not check:
        print("Cannot do database minimization. ")
        return None, time_profile
    if not vm.done:
        print("Some problem while view minimization. Aborting extraction!")
        return None, time_profile

    wc = WhereClause(connectionHelper,
                     key_lists,
                     core_relations,
                     vm.global_other_info_dict,
                     vm.global_result_dict,
                     vm.global_min_instance_dict)
    check = wc.doJob(query)
    time_profile.update_for_where_clause(wc.local_elapsed_time)
    if not check:
        print("Cannot find where clause.")
        return None, time_profile
    if not wc.done:
        print("Some error while where clause extraction. Aborting extraction!")
        return None, time_profile

    pj = Projection(connectionHelper,
                    wc.global_attrib_types,
                    core_relations,
                    wc.filter_predicates,
                    wc.global_join_graph,
                    wc.global_all_attribs)
    check = pj.doJob(query)
    time_profile.update_for_projection(pj.local_elapsed_time)
    if not check:
        print("Cannot find projected attributes. ")
        return None, time_profile
    if not pj.done:
        print("Some error while projection extraction. Aborting extraction!")
        return None, time_profile

    gb = GroupBy(connectionHelper,
                 wc.global_attrib_types,
                 core_relations,
                 wc.filter_predicates,
                 wc.global_all_attribs,
                 wc.global_join_graph,
                 pj.projected_attribs)
    check = gb.doJob(query)
    time_profile.update_for_group_by(gb.local_elapsed_time)
    if not check:
        print("Cannot find group by attributes. ")

    if not gb.done:
        print("Some error while group by extraction. Aborting extraction!")
        return None, time_profile

    agg = Aggregation(connectionHelper,
                      wc.global_key_attributes,
                      wc.global_attrib_types,
                      core_relations,
                      wc.filter_predicates,
                      wc.global_all_attribs,
                      wc.global_join_graph,
                      pj.projected_attribs,
                      gb.has_groupby,
                      gb.group_by_attrib)
    check = agg.doJob(query)
    time_profile.update_for_aggregate(agg.local_elapsed_time)
    if not check:
        print("Cannot find aggregations.")
    if not agg.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None, time_profile

    ob = OrderBy(connectionHelper,
                 wc.global_key_attributes,
                 wc.global_attrib_types,
                 core_relations,
                 wc.filter_predicates,
                 wc.global_all_attribs,
                 wc.global_join_graph,
                 pj.projected_attribs,
                 pj.projection_names,
                 agg.global_aggregated_attributes)

    ob.doJob(query)
    time_profile.update_for_order_by(ob.local_elapsed_time)
    if not ob.has_orderBy:
        print("Cannot find aggregations.")
    if not ob.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None, time_profile

    lm = Limit(connectionHelper,
               wc.global_attrib_types,
               wc.global_key_attributes,
               core_relations,
               wc.filter_predicates,
               wc.global_all_attribs,
               gb.group_by_attrib)

    lm.doJob(query)
    time_profile.update_for_limit(lm.local_elapsed_time)
    if lm.limit is None:
        print("Cannot find limit.")
    if not lm.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None, time_profile

    q_generator = QueryStringGenerator(connectionHelper)
    eq = q_generator.generate_query_string(core_relations, wc, pj, gb, agg, ob, lm)
    print("extracted query without NEP:\n", eq)

    '''
    if eq is not None or eq != '':
        
        nep = NEP(connectionHelper, core_relations, cs2.sizes,
                  global_pk_dict,
                  wc.global_all_attribs,
                  wc.global_attrib_types,
                  wc.filter_predicates,
                  wc.global_key_attributes,
                  q_generator)
        check = nep.doJob([query, eq])
        if not check:
            print("NEP is not there.")
        if not nep.done:
            print("Some error while extrating NEPs. Aborting extraction!")
            return None
    else:
        nep = None
        
    eq = nep.Q_E
   '''
    nep = None  # disable NEP now
    # time_profile = ElapsedTime(cs2, vm, wc, pj, gb, agg, ob, lm, nep, vm.app)

    return eq, time_profile
