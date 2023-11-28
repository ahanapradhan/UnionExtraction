import copy

from .util.utils import get_dummy_val_for, get_val_plus_delta, get_format, get_char, isQ_result_empty
from ..refactored.abstract.GenerationPipeLineBase import GenerationPipeLineBase
from ..src.util.constants import COUNT, COUNT_STAR, NO_ORDER, SUM


class CandidateAttribute:
    """docstring for CandidateAttribute"""

    def __init__(self, logger, attrib, aggregation, dependency, dependencyList, attrib_dependency, index, name):
        self.attrib = attrib
        self.aggregation = aggregation
        self.dependency = dependency
        self.dependencyList = copy.deepcopy(dependencyList)
        self.attrib_dependency = copy.deepcopy(attrib_dependency)
        self.index = index
        self.name = name
        self.logger = logger

    def debug_print(self):
        self.logger.debug(self.attrib)
        self.logger.debug(self.aggregation)
        self.logger.debug(self.dependency)
        self.logger.debug(self.dependencyList)
        self.logger.debug(self.attrib_dependency)
        self.logger.debug(self.index)
        self.logger.debug('')


def tryConvert(logger, val):
    temp = 0
    changed = False
    try:
        temp = int(val)
        changed = True
    except ValueError as e:
        logger.debug("Not int error, ", e)
        temp = val
    if not changed:
        try:
            temp = float(val)
            changed = True
        except ValueError as e:
            logger.debug("Not int error, ", e)
            temp = val
    return temp


def checkOrdering(logger, obj, result):
    if len(result) < 2:
        return None
    reference_value = tryConvert(logger, result[1][obj.index])
    for i in range(2, len(result)):
        logger.debug(result)
        if tryConvert(logger, result[i][obj.index]) != reference_value:
            return 'asc' if tryConvert(logger, result[i][obj.index]) > reference_value else 'desc'
    return None


class OrderBy(GenerationPipeLineBase):

    def __init__(self, connectionHelper, global_key_attributes, global_attrib_types, core_relations, filter_predicates,
                 global_all_attribs, join_graph, projected_attribs, global_projection_names, global_dependencies,
                 global_aggregated_attributes, global_min_instance_dict):
        super().__init__(connectionHelper, "Order By", core_relations, global_all_attribs, global_attrib_types,
                         join_graph, filter_predicates, global_min_instance_dict)
        self.global_projection_names = global_projection_names
        self.projected_attribs = projected_attribs
        self.global_aggregated_attributes = global_aggregated_attributes
        self.global_key_attributes = global_key_attributes
        self.orderby_list = []
        self.global_dependencies = global_dependencies
        self.orderBy_string = ''
        self.has_orderBy = True

    def doExtractJob(self, query, attrib_types_dict, filter_attrib_dict):
        # ORDERBY ON PROJECTED COLUMNS ONLY
        # ASSUMING NO ORDER ON JOIN ATTRIBUTES
        cand_list = self.construct_candidate_list()
        # CHECK ORDER BY ON COUNT
        self.orderBy_string = self.remove_equality_predicates(cand_list, filter_attrib_dict, query)
        self.check_order_by_on_count(cand_list, self.orderBy_string, filter_attrib_dict, query)
        self.has_orderBy = self.orderBy_string or self.orderby_list
        return True

    def check_order_by_on_count(self, cand_list, curr_orderby, filter_attrib_dict, query):
        for elt in cand_list:
            if COUNT not in elt.aggregation:
                continue
            for i in range(len(self.orderby_list) + 1):
                temp_orderby_list = []
                for j in range(i):
                    temp_orderby_list.append(self.orderby_list[j])
                order = self.generateData(elt, temp_orderby_list, filter_attrib_dict, curr_orderby, query)
                if order is None:
                    break
                else:
                    if order != NO_ORDER:
                        self.orderby_list.insert(i, (elt, order))
                        curr_orderby += COUNT_STAR + " " + order + ", "
                        break

    def remove_equality_predicates(self, cand_list, filter_attrib_dict, query):
        # REMOVE ELEMENTS WITH EQUALITY FILTER PREDICATES
        remove_list = []
        for elt in cand_list:
            for entry in self.global_filter_predicates:
                if elt.attrib == entry[1] and (entry[2] == '=' or entry[2] == 'equal') and not (
                        SUM in elt.aggregation or COUNT in elt.aggregation):
                    remove_list.append(elt)
        for elt in remove_list:
            cand_list.remove(elt)
        curr_orderby = ""
        while self.has_orderBy and cand_list:
            remove_list = []
            self.has_orderBy = False
            for elt in cand_list:
                if COUNT in elt.aggregation:
                    continue
                order = self.generateData(elt, self.orderby_list, filter_attrib_dict, curr_orderby, query)
                if order is None:
                    remove_list.append(elt)
                elif order != NO_ORDER:
                    self.has_orderBy = True
                    self.orderby_list.append((elt, order))
                    curr_orderby += elt.name + " " + order + ", "
                    remove_list.append(elt)
                    break
            for elt in remove_list:
                cand_list.remove(elt)
        return curr_orderby

    def construct_candidate_list(self):
        cand_list = []
        for i in range(len(self.global_aggregated_attributes)):
            dependencyList = []
            for j in range(len(self.global_aggregated_attributes)):
                if j != i and self.global_aggregated_attributes[i][0] == \
                        self.global_aggregated_attributes[j][0]:
                    dependencyList.append((self.global_aggregated_attributes[j]))
            cand_list.append(CandidateAttribute(self.logger, self.global_aggregated_attributes[i][0],
                                                self.global_aggregated_attributes[i][1], not (not dependencyList),
                                                dependencyList, self.global_dependencies[i], i,
                                                self.global_projection_names[i]))
        for i in cand_list:
            i.debug_print()
        return cand_list

    def generateData(self, obj, orderby_list, filter_attrib_dict, curr_orderby, query):
        attrib_types_dict = {}

        for entry in self.global_attrib_types:
            attrib_types_dict[(entry[0], entry[1])] = entry[2]
        # check if it is a key attribute, #NO CHECKING ON KEY ATTRIBUTES
        self.logger.debug(obj.attrib)
        key_elt = None
        if obj.attrib in self.global_key_attributes:
            for elt in self.global_join_graph:
                if obj.attrib in elt:
                    key_elt = elt

        if not obj.dependency:
            # if obj.name not in self.global_attrib_dict['order by']:
            #    self.global_attrib_dict['order by'].append(obj.name)
            # ATTRIBUTES TO GET SAME VALUE FOR BOTH ROWS
            # EASY AS KEY ATTRIBUTES ARE NOT THERE IN ORDER AS PER ASSUMPTION SO FAR
            # IN CASE OF COUNT ---
            # Fill 3 rows in any one table (with a a b values) and 2 in all others (with a b values) in D1
            # Fill 3 rows in any one table (with a b b values) and 2 in all others (with a b values) in D2
            same_value_list = []
            for elt in orderby_list:
                for i in elt[0].attrib_dependency:
                    key_f = None
                    for j in self.global_join_graph:
                        if i[1] in j:
                            key_f = j
                    self.logger.debug("Key: ", key_f)
                    if key_f:
                        for in_e in key_f:
                            same_value_list.append(("check", in_e))
                    else:
                        same_value_list.append(i)
            no_of_db = 2
            order = [None, None]
            # For this attribute (obj.attrib), fill all tables now
            for k in range(no_of_db):
                # Truncate all core relations
                self.truncate_core_relations()
                for j in range(len(self.core_relations)):
                    first_rel = self.core_relations[0]
                    tabname_inner = self.core_relations[j]
                    attrib_list_inner = self.global_all_attribs[j]
                    insert_rows = []
                    insert_values1 = []
                    insert_values2 = []
                    att_order = '('
                    flag = False
                    for attrib_inner in attrib_list_inner:
                        if not flag:
                            att_order += attrib_inner + ","
                        if 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
                            if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                first = filter_attrib_dict[(tabname_inner, attrib_inner)][0]
                                second = min \
                                    (get_val_plus_delta('date', first, 1),
                                     filter_attrib_dict[(tabname_inner, attrib_inner)][1])
                            else:
                                first = get_dummy_val_for('date')
                                second = get_val_plus_delta('date', first, 1)
                            first = get_format('date', first)
                            second = get_format('date', second)
                        elif ('int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in
                              attrib_types_dict
                              [(tabname_inner, attrib_inner)]):
                            # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
                            if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                first = filter_attrib_dict[(tabname_inner, attrib_inner)][0]
                                second = min(get_val_plus_delta('int', first, 1),
                                             filter_attrib_dict[(tabname_inner, attrib_inner)][1])
                            else:
                                first = get_dummy_val_for('int')
                                second = get_val_plus_delta('int', first, 1)
                        else:
                            if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                # EQUAL FILTER WILL NOT COME HERE
                                if '_' in filter_attrib_dict[(tabname_inner, attrib_inner)]:
                                    string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
                                    first = string.replace('_', get_char(get_dummy_val_for('char')))
                                    string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
                                    second = string.replace('_', get_char(
                                        get_val_plus_delta('char', get_dummy_val_for('char'), 1)))
                                else:
                                    string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
                                    first = string.replace('%', get_char(get_dummy_val_for('char')), 1)
                                    string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
                                    second = string.replace('%', get_char(
                                        get_val_plus_delta('char', get_dummy_val_for('char'), 1)), 1)
                                first.replace('%', '')
                                second.replace('%', '')
                            else:
                                first = get_char(get_dummy_val_for('char'))
                                second = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), 1))
                        insert_values1.append(first)
                        insert_values2.append(second)
                        if k == no_of_db - 1 and (any([(attrib_inner in i) for i in
                                                       obj.attrib_dependency]) or 'Count' in obj.aggregation) or (
                                k == no_of_db - 1 and key_elt and attrib_inner in key_elt):
                            # swap first and second
                            self.logger.debug("Swapping", attrib_inner)
                            self.logger.debug("Attribute Order", att_order)
                            self.logger.debug("Prev", insert_values1, insert_values2)
                            insert_values2[-1], insert_values1[-1] = insert_values1[-1], insert_values2[-1]
                            self.logger.debug("New", insert_values1, insert_values2)
                        self.logger.debug("same value", same_value_list)
                        if any([(attrib_inner in i) for i in same_value_list]):
                            insert_values2[-1] = insert_values1[-1]
                    flag = True
                    if COUNT in obj.aggregation and tabname_inner == first_rel:
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values2))
                    else:
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values2))
                    self.logger.debug(att_order)
                    self.logger.debug("Insert 1", insert_values1)
                    self.logger.debug("Insert 2", insert_values2)
                    self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)
                # nr = self.app.doJob("select * from lineitem;")
                # for i in nr:
                #     self.logger.debug(i)
                new_result = self.app.doJob(query)
                self.logger.debug("New Result", k, new_result)
                if isQ_result_empty(new_result):
                    self.logger.error('some error in generating new database. '
                                      'Result is empty. Can not identify Ordering')
                    return None
                if len(new_result) == 2:
                    return None
                order[k] = checkOrdering(self.logger, obj, new_result)
                self.logger.debug("Order", k, order)
            if order[0] is not None and order[1] is not None and order[0] == order[1]:
                return order[0]
            else:
                return NO_ORDER
        return NO_ORDER
