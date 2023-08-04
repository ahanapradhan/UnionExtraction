import copy

from ...refactored.aggregation import Aggregation
from ...refactored.cs2 import Cs2
from ...refactored.groupby_clause import GroupBy
from ...refactored.projection import Projection
from ...refactored.util.utils import get_datatype, get_min_and_max_val, get_format
from ...refactored.view_minimizer import ViewMinimizer
from ...refactored.where_clause import WhereClause


def func_assemble(global_select_op,
                  global_from_op,
                  global_where_op,
                  global_groupby_op):
    output = "Select " + global_select_op \
             + "\n" + "From " + global_from_op
    if global_where_op != '':
        output = output + "\n" + "Where " + global_where_op
    if global_groupby_op != '':
        output = output + "\n" + "Group By " + global_groupby_op
    output = output + ";"
    return output


def refine_Query(wc, pj, gb, agg,
                 global_select_op,
                 global_from_op,
                 global_where_op,
                 global_groupby_op,
                 global_output_list=[]):
    for i in range(len(agg.global_projected_attributes)):
        attrib = agg.global_projected_attributes[i]
        if attrib in wc.global_key_attributes and attrib in agg.global_groupby_attributes:
            if not ('Sum' in agg.global_aggregated_attributes[i][1] or 'Count' in
                    agg.global_aggregated_attributes[i][1]):
                agg.global_aggregated_attributes[i] = (agg.global_aggregated_attributes[i][0], '')
    temp_list = copy.deepcopy(agg.global_groupby_attributes)
    for attrib in temp_list:
        if attrib not in agg.global_projected_attributes:
            try:
                agg.global_groupby_attributes.remove(attrib)
            except:
                pass
            continue
        remove_flag = True
        for elt in agg.global_aggregated_attributes:
            if elt[0] == attrib and (not ('Sum' in elt[1] or 'Count' in elt[1])):
                remove_flag = False
                break
        if remove_flag:
            try:
                agg.global_groupby_attributes.remove(attrib)
            except:
                pass

    # UPDATE OUTPUTS
    first_occur = True
    for i in range(len(agg.global_groupby_attributes)):
        elt = agg.global_groupby_attributes[i]
        if first_occur:
            global_groupby_op = elt
            first_occur = False
        else:
            global_groupby_op = global_groupby_op + ", " + elt
    first_occur = True
    for i in range(len(agg.global_projected_attributes)):
        elt = agg.global_projected_attributes[i]
        global_output_list.append(copy.deepcopy(elt))
        if agg.global_aggregated_attributes[i][1] != '':
            if elt != '':
                suffix = '(' + elt + ')'
            else:
                suffix = ""
            elt = agg.global_aggregated_attributes[i][1] + suffix
            if 'Count' in agg.global_aggregated_attributes[i][1]:
                elt = agg.global_aggregated_attributes[i][1]
            global_output_list[-1] = copy.deepcopy(elt)
        if elt != pj.projection_names[i] and pj.projection_names[i] != '':
            elt = elt + ' as ' + pj.projection_names[i]
            global_output_list[-1] = copy.deepcopy(pj.projection_names[i])
        if first_occur:
            global_select_op = elt
            first_occur = False
        else:
            global_select_op = global_select_op + ", " + elt

    eq = func_assemble(global_select_op,
                       global_from_op,
                       global_where_op,
                       global_groupby_op)
    return eq, global_output_list


def extract(connectionHelper,
            query,
            all_relations,
            core_relations,
            key_lists):  # get core_relations, key_lists from from clause

    cs2 = Cs2(connectionHelper, all_relations, core_relations, key_lists)
    cs2.doJob(query)

    vm = ViewMinimizer(connectionHelper, core_relations, cs2.passed)
    check = vm.doJob(query)
    if not check:
        print("Cannot do database minimization. ")
        return None
    if not vm.done:
        print("Some problem while view minimization. Aborting extraction!")
        return None

    wc = WhereClause(connectionHelper,
                     key_lists,
                     core_relations,
                     vm.global_other_info_dict,
                     vm.global_result_dict,
                     vm.global_min_instance_dict)
    check = wc.doJob(query)
    if not check:
        print("Cannot find where clause.")
        return None
    if not wc.done:
        print("Some error while where clause extraction. Aborting extraction!")
        return None

    pj = Projection(connectionHelper,
                    wc.global_attrib_types,
                    core_relations,
                    wc.filter_predicates,
                    wc.global_join_graph,
                    wc.global_all_attribs)
    check = pj.doJob(query)
    if not check:
        print("Cannot find projected attributes. ")
        return None
    if not pj.done:
        print("Some error while projection extraction. Aborting extraction!")
        return None

    gb = GroupBy(connectionHelper,
                 wc.global_attrib_types,
                 core_relations,
                 wc.filter_predicates,
                 wc.global_all_attribs,
                 wc.global_join_graph,
                 pj.projected_attribs)
    check = gb.doJob(query)
    if not check:
        print("Cannot find group by attributes. ")

    if not gb.done:
        print("Some error while group by extraction. Aborting extraction!")
        return None

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
    if not check:
        print("Cannot find aggregations.")
    if not agg.done:
        print("Some error while extrating aggregations. Aborting extraction!")
        return None

    eq = generate_query_string(agg, core_relations, gb, pj, wc)
    return eq


def generate_query_string(agg, core_relations, gb, pj, wc):
    global_select_op = ''
    global_groupby_op = ''
    global_from_op = ", ".join(core_relations)
    joins = []
    for edge in wc.global_join_graph:
        joins.append(" = ".join(edge))
    global_where_op = " and ".join(joins)
    if len(joins) > 0 and len(wc.filter_predicates) > 0:
        global_where_op += " and "
    global_where_op = add_filters(global_where_op, wc)
    eq, output_list = refine_Query(wc, pj, gb, agg,
                                   global_select_op,
                                   global_from_op,
                                   global_where_op,
                                   global_groupby_op,
                                   [])
    return eq


def add_filters(global_where_op, wc):
    filters = []
    for pred in wc.filter_predicates:
        tab_col = tuple(pred[:2])
        pred_op = pred[1] + " "
        datatype = get_datatype(wc.global_attrib_types, tab_col)
        if pred[2] != ">=" and pred[2] != "<=" and pred[2] != "range":
            if pred[2] == "equal":
                pred_op += " = "
            else:
                pred_op += pred[2] + " "
            pred_op += get_format(datatype, pred[3])
        else:
            min_val, max_val = get_min_and_max_val(datatype)
            min_present = False
            max_present = False
            if pred[3] == min_val:  # no min val
                min_present = True
            if pred[4] == max_val:  # no max val
                max_present = True

            if min_present and not max_present:
                pred_op += " <= " + get_format(datatype, pred[4])
            elif not min_present and max_present:
                pred_op += " >= " + get_format(datatype, pred[3])
            elif not min_present and not max_present:
                pred_op += " >= " + get_format(datatype, pred[3]) + " and " + pred[1] + " <= " + get_format(datatype, pred[4])

        filters.append(pred_op)
    global_where_op += " and ".join(filters)
    return global_where_op
