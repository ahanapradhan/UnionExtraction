import copy

import frozenlist

from .dataclass.genPipeline_context import GenPipelineContext
from ...src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase
from ..util.constants import NON_TEXT_TYPES
from ...src.util.constants import COUNT, NO_ORDER, SUM, ORPHAN_COLUMN
from ...src.util.utils import get_unused_dummy_val, get_dummy_val_for, \
    get_val_plus_delta, get_format, get_char


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
        self.logger.debug(f"attrib: {self.attrib}")
        self.logger.debug(f"aggregation: {self.aggregation}")
        self.logger.debug(f"dependency: {self.dependency}")
        self.logger.debug(f"dependency list: {self.dependencyList}")
        self.logger.debug(f"attrib_dependency: {self.attrib_dependency}")
        self.logger.debug(f"projection index: {self.index}")
        self.logger.debug('')


def tryConvert(logger, val):
    changed = False
    try:
        temp = int(val)
        changed = True
    except ValueError as e:
        # logger.debug("Not int error, ", e)
        temp = val
    if not changed:
        try:
            temp = float(val)
            changed = True
        except ValueError as e:
            # logger.debug("Not int error, ", e)
            temp = val
    return temp


def check_sort_order(logger, lst):
    if all(tryConvert(logger, lst[i]) <= tryConvert(logger, lst[i + 1]) for i in range(len(lst) - 1)):
        return "asc"
    elif all(tryConvert(logger, lst[i]) >= tryConvert(logger, lst[i + 1]) for i in range(len(lst) - 1)):
        return "desc"
    else:
        return NO_ORDER


class OrderBy(GenerationPipeLineBase):

    def __init__(self, connectionHelper,
                 genPipelineCtx: GenPipelineContext,
                 pgao_Ctx):
        super().__init__(connectionHelper, "Order By", genPipelineCtx)
        self.values_used = []
        self.global_projection_names = pgao_Ctx.projection_names
        self.projected_attribs = pgao_Ctx.projected_attribs
        self.global_aggregated_attributes = pgao_Ctx.aggregated_attributes
        self.orderby_list = []
        self.global_dependencies = pgao_Ctx.projection_dependencies
        self.orderBy_string = ''
        self.has_orderBy = True
        self.joined_attrib_valDict = {}

    def doExtractJob(self, query):
        # ORDERBY ON PROJECTED COLUMNS ONLY
        # ASSUMING NO ORDER ON JOIN ATTRIBUTES
        cand_list = self.construct_candidate_list()
        self.logger.debug("candidate list: ", cand_list)
        # CHECK ORDER BY ON COUNT
        self.orderBy_string = self.get_order_by(cand_list, query)
        self.has_orderBy = True if len(self.orderby_list) else False
        self.logger.debug("order by string: ", self.orderBy_string)
        self.logger.debug("order by list: ", self.orderby_list)
        return True

    def check_order_by_on_count(self, cand_list, query):
        for elt in cand_list:
            if COUNT not in elt.aggregation:
                self.logger.debug("Skipping, NO COUNT")
                continue
            for i in range(len(self.orderby_list) + 1):
                temp_orderby_list = []
                for j in range(i):
                    temp_orderby_list.append(self.orderby_list[j])
                order = self.generateData(elt, temp_orderby_list, query)
                if order is None:
                    break
                else:
                    if order != NO_ORDER:
                        self.logger.debug("Order by on count", order)
                        self.orderby_list.insert(i, (elt, order))
                        self.logger.debug("Order by list", self.orderby_list)
                        self.orderBy_string += elt.name + " " + order + ", "
                        break

    def get_order_by(self, cand_list, query):
        # REMOVE ELEMENTS WITH EQUALITY FILTER PREDICATES
        remove_list = []
        for elt in cand_list:
            for entry in self.global_filter_predicates:
                if elt.attrib == entry[1] and (entry[2] == '=' or entry[2] == 'equal') and not (
                        SUM in elt.aggregation or COUNT in elt.aggregation):
                    remove_list.append(elt)
        for elt in remove_list:
            cand_list.remove(elt)
        curr_orderby = []
        while self.has_orderBy and cand_list:
            remove_list = []
            self.has_orderBy = False
            row_num = 2
            for elt in cand_list:
                if COUNT in elt.aggregation:
                    row_num = 3
            for elt in cand_list:
                order = self.generateData(elt, self.orderby_list, query, row_num)
                if order is None or elt.name == ORPHAN_COLUMN:
                    remove_list.append(elt)
                elif order != NO_ORDER:
                    self.has_orderBy = True
                    self.orderby_list.append((elt, order))
                    curr_orderby.append(f"{elt.name} {order}")
                    remove_list.append(elt)
                    break
            for elt in remove_list:
                cand_list.remove(elt)
        curr_orderby_str = ", ".join(curr_orderby)

        self.logger.debug(curr_orderby_str)
        return curr_orderby_str

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
        return cand_list

    def is_part_of_output(self, tab, attrib):
        is_output = False
        for edge in self.global_join_graph:
            if attrib in edge:
                for o_attrib in edge:
                    for agg in self.global_dependencies:
                        for tup in agg:
                            if o_attrib in tup:
                                is_output = True
                                break
        for agg in self.global_dependencies:
            if (tab, attrib) in agg:
                is_output = True
                break
        return is_output

    def generateData(self, obj, orderby_list, query, row_num):
        # check if it is a key attribute, #NO CHECKING ON KEY ATTRIBUTES
        self.logger.debug(obj.attrib)
        key_elt = None
        if obj.attrib in self.joined_attribs:
            for elt in self.global_join_graph:
                if obj.attrib in elt:
                    key_elt = elt

        if not obj.dependency:
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
                    # self.logger.debug("Key: ", key_f)
                    if key_f:
                        for in_e in key_f:
                            same_value_list.append(("check", in_e))
                    else:
                        same_value_list.append(i)
            no_of_db = 2
            order = [None, None]
            # For this attribute (obj.attrib), fill all tables now
            for k in range(no_of_db):
                self.truncate_core_relations()
                self.values_used.clear()
                for tabname_inner in self.core_relations:
                    attrib_list_inner = self.global_all_attribs[tabname_inner]
                    insert_rows, insert_values1, insert_values2 = [], [], []
                    attrib_list_str = ",".join(attrib_list_inner)
                    att_order = f"({attrib_list_str})"
                    for attrib_inner in attrib_list_inner:
                        datatype = self.get_datatype((tabname_inner, attrib_inner))
                        if self.is_part_of_output(tabname_inner, attrib_inner):
                            if datatype in NON_TEXT_TYPES:
                                first, second = self.get_non_text_attrib(datatype, attrib_inner, tabname_inner)
                            else:
                                first, second = self.get_text_value(attrib_inner, tabname_inner)
                        else:
                            first = self.get_dmin_val(attrib_inner, tabname_inner)
                            second = get_val_plus_delta(datatype, first,
                                                        1) if attrib_inner in self.joined_attribs else first  # first  # get_val_plus_delta(datatype, first,
                            #            1) if attrib_inner in self.joined_attribs else first
                        insert_values1.append(first)
                        insert_values2.append(second)
                        if k == no_of_db - 1 and (any([(attrib_inner in i) for i in
                                                       obj.attrib_dependency]) or 'Count' in obj.aggregation) or (
                                k == no_of_db - 1 and key_elt and attrib_inner in key_elt):  # \
                            # or (k == no_of_db - 1 and key_elt and attrib_inner in key_elt):
                            # swap first and second
                            insert_values2[-1], insert_values1[-1] = insert_values1[-1], insert_values2[-1]

                        if any([(attrib_inner in i) for i in same_value_list]):
                            insert_values2[-1] = insert_values1[-1]
                    if row_num == 3:
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values2))
                    else:
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values2))

                    self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)

                new_result = self.app.doJob(query)
                self.joined_attrib_valDict.clear()
                self.logger.debug("New Result", k, new_result)
                if self.app.isQ_result_empty(new_result):
                    self.logger.error('some error in generating new database. '
                                      'Result is empty. Can not identify Ordering')
                    return None
                if len(new_result) == 2:
                    return None
                data = self.app.get_all_nullfree_rows(new_result)
                check_res = []
                for d in data:
                    check_res.append(d[obj.index])
                order[k] = check_sort_order(self.logger, check_res)
                # order[k] = checkOrdering(self.logger, obj, new_result)
                self.logger.debug("Order", k, order)
            if order[0] is not None and order[1] is not None and order[0] == order[1]:
                self.logger.debug("Order Found", order[0])
                return order[0]
            else:
                return NO_ORDER
        return NO_ORDER

    def get_text_value(self, attrib_inner, tabname_inner):
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            # EQUAL FILTER WILL NOT COME HERE
            s_val_text = self.get_s_val_for_textType(attrib_inner, tabname_inner)
            if '_' in s_val_text:
                string = copy.deepcopy(s_val_text)
                first = string.replace('_', get_char(get_dummy_val_for('char')))
                string = copy.deepcopy(s_val_text)
                second = string.replace('_', get_char(
                    get_val_plus_delta('char', get_dummy_val_for('char'), 1)))
            else:
                string = copy.deepcopy(s_val_text)
                first = string.replace('%', get_char(get_dummy_val_for('char')), 1)
                string = copy.deepcopy(s_val_text)
                second = string.replace('%', get_char(
                    get_val_plus_delta('char', get_dummy_val_for('char'), 1)), 1)
            first = first.replace('%', '')
            second = second.replace('%', '')
        else:
            first = get_char(get_dummy_val_for('char'))
            second = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), 1))
        return first, second

    def get_non_text_attrib(self, datatype, attrib_inner, tabname_inner):
        for edge in self.global_join_graph:
            if attrib_inner in edge:
                edge_key = frozenlist.FrozenList(edge)
                edge_key.freeze()
                if edge_key in self.joined_attrib_valDict.keys():
                    return self.joined_attrib_valDict[edge_key][0], self.joined_attrib_valDict[edge_key][1]
        # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
        if datatype != 'date':
            datatype = 'int'
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            first = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            second = min(get_val_plus_delta(datatype, first, 1),
                         self.filter_attrib_dict[(tabname_inner, attrib_inner)][1])
        else:
            first = get_unused_dummy_val(datatype, self.values_used)
            second = get_val_plus_delta(datatype, first, 1)
        self.values_used.extend([first, second])
        for edge in self.global_join_graph:
            if attrib_inner in edge:
                edge_key = frozenlist.FrozenList(edge)
                edge_key.freeze()
                if edge_key not in self.joined_attrib_valDict.keys():
                    if datatype == 'date':
                        first = get_format('date', first)
                        second = get_format('date', second)
                    self.joined_attrib_valDict[edge_key] = [first, second]
        return first, second

    def get_date_values(self, attrib_inner, tabname_inner):
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            first = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            second = min(get_val_plus_delta('date', first, 1),
                         self.filter_attrib_dict[(tabname_inner, attrib_inner)][1])
        else:
            first = get_dummy_val_for('date')
            second = get_val_plus_delta('date', first, 1)
        first = get_format('date', first)
        second = get_format('date', second)
        return first, second
