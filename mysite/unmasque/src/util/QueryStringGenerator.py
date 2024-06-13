import ast
import copy
from _decimal import Decimal

import frozenlist
from frozenlist._frozenlist import FrozenList

from .aoa_utils import remove_item_from_list, find_tables_from_predicate, get_constants_for, get_tab, get_attrib, \
    get_LB, get_op, get_UB, add_item_to_list
from ..core.abstract.GenerationPipeLineBase import NUMBER_TYPES, NON_TEXT_TYPES
from ..core.factory.ExecutableFactory import ExecutableFactory
from ..util.Log import Log
from ..util.constants import COUNT, SUM, max_str_len, AVG, MIN, MAX, ORPHAN_COLUMN
from ..util.utils import get_format, get_min_and_max_val, get_val_plus_delta


def append_clause(output, clause, param):
    if param is not None and param != '':
        output = f"{output} \n {clause} {param}"
    return output


class QueryDetails:
    def __init__(self):
        self.core_relations = []

        self.eq_join_predicates = []
        self.filter_in_predicates = []
        self.multi_attrib_or_predicates = []
        self.arithmetic_filters = []
        self.filter_not_in_predicates = []
        self.aoa_less_thans = []
        self.all_aoa = []
        self.join_edges = []
        self.or_predicates = []

        self.projection_names = []
        self.global_projected_attributes = []
        self.global_groupby_attributes = []
        self.global_aggregated_attributes = []
        self.global_key_attributes = []

        self.select_op = ''
        self.from_op = ''
        self.where_op = ''
        self.group_by_op = ''
        self.order_by_op = ''
        self.limit_op = ''

    def makeCopy(self, other):
        self.core_relations = other.core_relations
        self.eq_join_predicates = other.eq_join_predicates
        self.filter_in_predicates = other.filter_in_predicates
        self.arithmetic_filters = other.arithmetic_filters
        self.all_aoa = other.all_aoa
        self.join_edges = other.join_edges
        self.projection_names = other.projection_names
        self.global_projected_attributes = other.global_projected_attributes
        self.global_groupby_attributes = other.global_groupby_attributes
        self.global_aggregated_attributes = other.global_aggregated_attributes

    def add_to_where_op(self, predicate):
        if self.where_op and predicate not in self.where_op:
            self.where_op = f'{self.where_op} and {predicate}'
        else:
            self.where_op = predicate

    def assembleQuery(self):
        output = ""
        output = append_clause(output, "Select", self.select_op)
        output = append_clause(output, "From", self.from_op)
        output = append_clause(output, "Where", self.where_op)
        output = append_clause(output, "Group By", self.group_by_op)
        output = append_clause(output, "Order By", self.order_by_op)
        output = append_clause(output, "Limit", self.limit_op)
        output = f"{output};"
        return output

    def optimize_arithmetic_filters(self):
        to_remove = []
        for ar_fil in self.arithmetic_filters:
            for fl_in in self.filter_in_predicates:
                if get_tab(ar_fil) == get_tab(fl_in) and get_attrib(ar_fil) == get_attrib(fl_in):
                    op = get_op(ar_fil)
                    if op in ['=', 'equal'] and get_LB(ar_fil) in get_LB(fl_in):
                        to_remove.append(ar_fil)
                    elif op == 'range' and (get_LB(ar_fil), get_UB(ar_fil)) in get_LB(fl_in):
                        to_remove.append(ar_fil)
        for mul in self.multi_attrib_or_predicates:
            for m_tup in mul:
                if m_tup in self.arithmetic_filters:
                    to_remove.append(m_tup)
        for t_r in to_remove:
            self.arithmetic_filters.remove(t_r)


def get_formatted_value(datatype, value):
    if isinstance(value, FrozenList):
        v_list = list(value)
        f_value = f"{', '.join(v_list)}"
        if len(v_list) > 1:
            f_value = f"({f_value})"
    elif isinstance(value, list):
        f_value = f"{', '.join(value)}"
        if len(value) > 1:
            f_value = f"({f_value})"
    else:
        f_value = get_format(datatype, value)
    return f_value


def get_join_nodes_from_edge(edge):
    nodes = edge.split("=")
    left_node = nodes[0].split(".")
    right_node = nodes[1].split(".")
    left = (left_node[0].strip(), left_node[1].strip())
    right = (right_node[0].strip(), right_node[1].strip())
    return left, right


class QueryStringGenerator:
    ROJ = ' RIGHT OUTER JOIN '
    LOJ = ' LEFT OUTER JOIN '
    join_map = {('l', 'l'): ' INNER JOIN ', ('l', 'h'): ROJ,
                ('h', 'l'): LOJ, ('h', 'h'): ' FULL OUTER JOIN '}

    AGGREGATES = [SUM, AVG, MIN, MAX, COUNT]

    def __init__(self, connectionHelper):
        self.connectionHelper = connectionHelper
        exeFactory = ExecutableFactory()
        self.app = exeFactory.create_exe(self.connectionHelper)
        self.__get_datatype = None
        self._queries = {}
        self._workingCopy = QueryDetails()
        self.logger = Log("Query String Generator", connectionHelper.config.log_level)

    def reset(self):
        self._workingCopy = QueryDetails()

    @property
    def filter_predicates(self):
        return self._workingCopy.arithmetic_filters

    @filter_predicates.setter
    def filter_predicates(self, value):
        if value not in self._workingCopy.arithmetic_filters:
            self._workingCopy.arithmetic_filters.append(value)

    @property
    def select_op(self):
        return self._workingCopy.select_op

    @select_op.setter
    def select_op(self, value):
        self._workingCopy.select_op = value

    @property
    def from_op(self):
        return self._workingCopy.from_op

    @from_op.setter
    def from_op(self, value):
        self._workingCopy.from_op = value

    @property
    def where_op(self):
        return self._workingCopy.where_op

    @where_op.setter
    def where_op(self, value):
        self._workingCopy.where_op = value

    @property
    def get_datatype(self):
        return self.__get_datatype

    @get_datatype.setter
    def get_datatype(self, func):
        self.__get_datatype = func

    @property
    def from_clause(self):
        return self._workingCopy.core_relations

    @from_clause.setter
    def from_clause(self, core_relations):
        self._workingCopy.core_relations = core_relations
        self._workingCopy.core_relations.sort()

    @property
    def algebraic_predicates(self):
        return NotImplementedError

    @algebraic_predicates.setter
    def algebraic_predicates(self, aoa):
        self._workingCopy.eq_join_predicates = aoa.algebraic_eq_predicates
        for pred in aoa.aoa_less_thans:
            self._workingCopy.all_aoa.append((pred[0], '<', pred[1]))
        for pred in aoa.aoa_predicates:
            self._workingCopy.all_aoa.append((pred[0], '<=', pred[1]))
        self._workingCopy.arithmetic_filters = aoa.arithmetic_eq_predicates + aoa.arithmetic_ineq_predicates

    @property
    def limit(self):
        return NotImplementedError

    @limit.setter
    def limit(self, lm_obj):
        self._workingCopy.limit_op = str(lm_obj.limit) if lm_obj.limit is not None else ''

    @property
    def pgaoCtx(self):
        return NotImplementedError

    @pgaoCtx.setter
    def pgaoCtx(self, value):
        self._workingCopy.global_key_attributes = value.joined_attribs
        self._workingCopy.projection_names = value.projection_names
        self._workingCopy.global_aggregated_attributes = value.aggregated_attributes
        self._workingCopy.global_groupby_attributes = value.group_by_attrib
        self._workingCopy.global_projected_attributes = value.projected_attribs
        self._workingCopy.order_by_op = value.orderby_string

    @property
    def arithmetic_disjunctions(self):
        return NotImplementedError

    @arithmetic_disjunctions.setter
    def arithmetic_disjunctions(self, remnants):
        if isinstance(remnants, tuple):
            if remnants not in self._workingCopy.filter_in_predicates:
                self._workingCopy.filter_in_predicates.append(remnants)
                tab, attrib, op, neg_val = remnants[0], remnants[1], remnants[2], remnants[3]
                self._workingCopy.arithmetic_filters.remove((tab, attrib, 'range',
                                                             min(neg_val[0][0], neg_val[1][0]),
                                                             max(neg_val[1][1], neg_val[0][1])))
        else:
            for pred in remnants.filter_in_predicates:
                if pred not in self._workingCopy.filter_in_predicates:
                    self._workingCopy.filter_in_predicates.append(pred)
            for pred in remnants.multi_attrib_or_predicates:
                if pred not in self._workingCopy.multi_attrib_or_predicates:
                    self._workingCopy.multi_attrib_or_predicates.append(pred)
        self._workingCopy.optimize_arithmetic_filters()

    @property
    def all_arithmetic_filters(self):
        uniq_preds = list(set(self._workingCopy.arithmetic_filters
                              + self._workingCopy.filter_in_predicates
                              + self._workingCopy.filter_not_in_predicates))
        return uniq_preds

    @all_arithmetic_filters.setter
    def all_arithmetic_filters(self, value):
        raise NotImplementedError

    @property
    def algebraic_inequalities(self):
        return self._workingCopy.all_aoa

    @algebraic_inequalities.setter
    def algebraic_inequalities(self, value):
        raise NotImplementedError

    @property
    def join_edges(self):
        return self._workingCopy.join_edges

    @join_edges.setter
    def join_edges(self, value):
        self._workingCopy.join_edges = value
        self._workingCopy.eq_join_predicates.clear()  # when join edges are assigned directly, old equi join
        # predicates are obsolete

    def rectify_projection(self, replace_dict):
        for key in replace_dict.keys():
            if key not in self._workingCopy.global_groupby_attributes:
                continue
            self._workingCopy.global_groupby_attributes[self._workingCopy.global_groupby_attributes.index(key)] \
                = replace_dict[key]
            self._workingCopy.order_by_op.replace(key, replace_dict[key])
            self._workingCopy.global_projected_attributes[self._workingCopy.global_projected_attributes.index(key)] \
                = replace_dict[key]

        agg_replace_dict = {}
        for i, agg_tuple in enumerate(self._workingCopy.global_aggregated_attributes):
            attrib = agg_tuple[0]
            if attrib in replace_dict.keys():
                replace_attrib = replace_dict[attrib]
                agg_replace_dict[i] = (replace_attrib, agg_tuple[1])
        for key in agg_replace_dict.keys():
            self._workingCopy.global_aggregated_attributes[key] = agg_replace_dict[key]

        return self._workingCopy.global_projected_attributes, self._workingCopy.global_groupby_attributes, \
            self._workingCopy.global_aggregated_attributes, self._workingCopy.order_by_op

    def updateExtractedQueryWithNEPVal(self, query, val):
        for elt in val:
            tab, attrib, op, neg_val = elt[0], elt[1], elt[2], elt[3]
            datatype = self.get_datatype((tab, attrib))
            if op == '<>':
                predicate, predicate_tuple = self._extract_nep_op_info(attrib, datatype, elt, neg_val, op, query, tab)
                self.filter_predicates = predicate_tuple
            elif op == 'IN':
                predicate = f"{tab}.{attrib} between {neg_val[0][0]} and {neg_val[0][1]} OR {tab}.{attrib} between {neg_val[1][0]} and {neg_val[1][1]}"
                self.arithmetic_disjunctions = elt
            else:
                predicate = ""

            self._workingCopy.add_to_where_op(predicate)

        Q_E = self.write_query()
        return Q_E

    def _extract_nep_op_info(self, attrib, datatype, elt, neg_val, op, query, tab):
        format_val = get_format(datatype, neg_val)
        if datatype == 'str':
            output = self._getStrFilterValue(query, elt[0], elt[1], elt[3], max_str_len)
            self.logger.debug(output)
            if '%' in output or '_' in output:
                predicate = f"{tab}.{attrib} NOT LIKE '{str(output)}' "
                self._remove_exact_NE_string_predicate(elt)
                predicate_tuple = (tab, attrib, 'NOT LIKE', output)
            else:
                predicate = f"{tab}.{attrib} {str(op)} \'{str(output)}\' "
                predicate_tuple = (tab, attrib, str(op), output)
        else:
            predicate = f"{tab}.{attrib} {str(op)} {format_val}"
            predicate_tuple = (tab, attrib, str(op), format_val)
        return predicate, predicate_tuple

    def __consolidate_nep_filters_for_not_in(self):
        t_remove = []
        nep_dict = {}
        for elt in self._workingCopy.arithmetic_filters:
            if elt[2] not in ['!=', '<>']:
                continue
            key = (elt[0], elt[1], elt[2])
            value = elt[3]
            if key not in nep_dict.keys():
                nep_dict[key] = [value]
            else:
                nep_dict[key].append(value)
        for key in nep_dict.keys():
            value = nep_dict[key]
            datatype = self.get_datatype((key[0], key[1]))
            if len(value) > 1:
                self.logger.debug("NOT IN FOUND..")
                for v in value:
                    t_remove.append((key[0], key[1], key[2], v))
                all_neps = [get_format(datatype, v) for v in value]
                if datatype in NON_TEXT_TYPES:
                    all_neps = sorted(all_neps)
                f_value = FrozenList(all_neps)
                f_value.freeze()
                self._workingCopy.filter_not_in_predicates.append((key[0], key[1], 'NOT IN', f_value, f_value))
                self.logger.debug(self._workingCopy.filter_not_in_predicates)

        for pred in t_remove:
            self._workingCopy.arithmetic_filters.remove(pred)
        return t_remove

    def rewrite_for_NEP(self):
        t_remove = self.__consolidate_nep_filters_for_not_in()
        for pred in t_remove:
            self._remove_exact_NE_string_predicate(pred)
        for notInPred in self._workingCopy.filter_not_in_predicates:
            pred = self.formulate_predicate_from_filter(notInPred)
            self._workingCopy.add_to_where_op(pred)
        self._workingCopy.where_op = self.__generate_where_clause()
        Q_E = self.write_query()
        return Q_E

    def updateWhereClause(self, predicate):
        self._workingCopy.add_to_where_op(predicate)
        return self.write_query()

    def __generate_where_clause(self) -> str:
        predicates = []
        if not len(self._workingCopy.join_edges):
            self.__generate_algebraice_eualities(predicates)
        else:
            predicates.extend(self._workingCopy.join_edges)
        self.__generate_algebraic_inequalities(predicates)

        self.__generate_arithmetic_pure_conjunctions(predicates)

        where_clause = "\n and ".join(predicates)
        self.logger.debug(where_clause)
        return where_clause

    def formulate_query_string(self):
        self._workingCopy.from_op = ", ".join(self._workingCopy.core_relations)
        self._workingCopy.where_op = self.__generate_where_clause()
        self.generate_groupby_select()
        eq = self.write_query()
        return eq

    def generate_groupby_select(self):
        self.__generate_group_by_clause()
        self.__generate_select_clause()

    def backup_query_before_new_generation(self, ref_query=None):  # make new query from the last memory
        lastQueryDetails = QueryDetails()
        lastQueryDetails.makeCopy(self._workingCopy)
        last_query = self.formulate_query_string()  # take backup of current working copy
        if ref_query is not None:
            ref_details = self._queries[hash(ref_query)][1]
            self._workingCopy.makeCopy(ref_details)
        return last_query

    def write_query(self) -> str:
        self.logger.debug(f"Select: {self._workingCopy.select_op}")
        self.logger.debug(f"From: {self._workingCopy.from_op}")
        self.logger.debug(f"Where: {self._workingCopy.where_op}")
        self.logger.debug(f"Group by: {self._workingCopy.group_by_op}")
        self.logger.debug(f"Order by: {self._workingCopy.order_by_op}")
        self.logger.debug(f"Limit: {self._workingCopy.limit_op}")

        query_string = self._workingCopy.assembleQuery()
        key = hash(query_string)
        self.logger.debug("hash key: ", key)
        if key not in self._queries:
            self._queries[key] = (query_string, copy.deepcopy(self._workingCopy))
        self.logger.debug("query_dict: ", self._queries)
        return query_string

    def __generate_predicate_string_for_in_operator(self, tab, attrib, values):
        datatype = self.get_datatype((tab, attrib))
        predicates = []
        single_value_set = []
        for v in values:
            if isinstance(v, tuple):
                elt = [tab, attrib, 'range', v[0], v[1]]
                predicates.append(self.formulate_predicate_from_filter(elt))
            else:
                single_value_set.append(get_format(datatype, v))
        f_values = get_formatted_value(datatype, single_value_set)
        op = 'IN' if len(single_value_set) > 1 else '='
        if len(single_value_set):
            predicates.append(f"{tab}.{attrib} {op} {f_values}")
        return " OR ".join(predicates)

    def formulate_predicate_from_filter(self, elt):
        tab, attrib, op, lb, ub = elt[0], elt[1], str(elt[2]).strip().lower(), elt[3], elt[-1]
        if op == 'in':
            predicate = self.__generate_predicate_string_for_in_operator(tab, attrib, lb)
            predicate = f"({predicate})" if "OR" in predicate else predicate
            return predicate
        datatype = self.get_datatype((tab, attrib))
        f_lb = get_formatted_value(datatype, lb)
        f_ub = get_formatted_value(datatype, ub)

        if op == 'range':
            predicate = ''
            i_min, i_max = get_min_and_max_val(datatype)
            if datatype == 'numeric':
                f_lb, f_ub = round(Decimal(lb), 2), round(Decimal(ub), 2)
                i_min, i_max = round(Decimal(i_min), 2), round(Decimal(i_max), 2)
            if lb == ub:
                predicate = f"{tab}.{attrib} = {f_lb}"
            if lb <= i_min:
                predicate = f"{tab}.{attrib} <= {f_ub}"
            if ub >= i_max:
                predicate = f"{tab}.{attrib} >= {f_lb}"
            if predicate == '':
                predicate = f"{tab}.{attrib} between {f_lb} and {f_ub}"
            if lb <= i_min and ub >= i_max:
                predicate = ''
        elif op == '>=':
            predicate = f"{tab}.{attrib} {op} {f_lb}"
        elif op in ['<=', '=', 'equal', 'like', 'not like', '<>', '!=', 'not in']:
            predicate = f"{tab}.{attrib} {str(op.replace('equal', '=')).upper()} {f_ub}"
        else:
            predicate = ''
        return predicate

    def __generate_algebraice_eualities(self, predicates):
        for eq_join in self._workingCopy.eq_join_predicates:
            self.logger.debug(f"Creating join clause for {eq_join}")
            join_edge = list(f"{item[0]}.{item[1]}" for item in eq_join if len(item) == 2)
            join_edge.sort()
            predicates.extend(f"{join_edge[i]} = {join_edge[i + 1]}" for i in range(len(join_edge) - 1))
        self.logger.debug(predicates)
        self._workingCopy.join_edges = copy.deepcopy(predicates)

    def __generate_algebraic_inequalities(self, predicates):
        for aoa in self._workingCopy.all_aoa:
            predicates.append(self.get_aoa_string(aoa))

    def __generate_arithmetic_pure_conjunctions(self, predicates):
        apc_predicates = self._workingCopy.filter_in_predicates \
                         + self._workingCopy.arithmetic_filters \
                         + self._workingCopy.filter_not_in_predicates
        for a_eq in apc_predicates:
            pred = self.formulate_predicate_from_filter(a_eq)
            add_item_to_list(pred, predicates)

        for p_tup in self._workingCopy.multi_attrib_or_predicates:
            strs = []
            for pred in p_tup:
                pred_str = self.formulate_predicate_from_filter(pred)
                strs.append(pred_str)
            or_pred = " OR ".join(strs)
            add_item_to_list(f"({or_pred})", predicates)

    def __optimize_group_by_attributes(self):
        for i in range(len(self._workingCopy.global_projected_attributes)):
            attrib = self._workingCopy.global_projected_attributes[i]
            if (attrib in self._workingCopy.global_key_attributes
                    and attrib in self._workingCopy.global_groupby_attributes):
                agg_op = self._workingCopy.global_aggregated_attributes[i][1]
                if agg_op not in self.AGGREGATES:
                    self._workingCopy.global_aggregated_attributes[i] = (
                        self._workingCopy.global_aggregated_attributes[i][0], '')
        temp_list = copy.deepcopy(self._workingCopy.global_groupby_attributes)
        for attrib in temp_list:
            if attrib not in self._workingCopy.global_projected_attributes:
                remove_item_from_list(attrib, self._workingCopy.global_groupby_attributes)
                continue
            remove_flag = True
            for elt in self._workingCopy.global_aggregated_attributes:
                if elt[0] == attrib and elt[1] not in self.AGGREGATES:
                    remove_flag = False
                    break
            if remove_flag:
                remove_item_from_list(attrib, self._workingCopy.global_groupby_attributes)

    def __generate_select_clause(self):
        for i in range(len(self._workingCopy.global_projected_attributes)):
            elt = self._workingCopy.global_projected_attributes[i]
            if self._workingCopy.global_aggregated_attributes[i][1] != '':
                if COUNT in self._workingCopy.global_aggregated_attributes[i][1]:
                    elt = self._workingCopy.global_aggregated_attributes[i][1]
                else:
                    elt = self._workingCopy.global_aggregated_attributes[i][1] + '(' + elt + ')'

            if elt != self._workingCopy.projection_names[i] and self._workingCopy.projection_names[i] != '':
                if self._workingCopy.projection_names[i] == ORPHAN_COLUMN:
                    elt = elt
                else:
                    elt = elt + ' as ' + self._workingCopy.projection_names[i]
            self._workingCopy.select_op = elt if not i else f'{self._workingCopy.select_op}, {elt}'

    def __generate_group_by_clause(self):
        self.__optimize_group_by_attributes()
        for i in range(len(self._workingCopy.global_groupby_attributes)):
            elt = self._workingCopy.global_groupby_attributes[i]
            # UPDATE OUTPUTS
            self._workingCopy.group_by_op = elt if not i else f'{self._workingCopy.group_by_op}, {elt}'

    def _remove_exact_NE_string_predicate(self, elt):
        while elt[1] in self._workingCopy.where_op:
            where_parts = self._workingCopy.where_op.split()
            attrib_index = where_parts.index(f"{elt[0]}.{elt[1]}")

            val = where_parts[attrib_index + 2]
            self.logger.debug(f"=== val: {val} to delete ===")
            if val.startswith("'") and val.endswith("'"):
                where_parts.pop(attrib_index + 2)
            elif val.startswith("'"):
                start_idx = attrib_index + 2
                check_idx = start_idx + 1
                while not where_parts[check_idx].endswith("'"):
                    check_idx += 1
                while check_idx >= start_idx:
                    where_parts.pop(check_idx)
                    check_idx -= 1

            where_parts.pop(attrib_index + 1)  # <>
            where_parts.pop(attrib_index)

            if "and" in where_parts[attrib_index - 1]:
                where_parts.pop(attrib_index - 1)  # for and
            self._workingCopy.where_op = " ".join(where_parts)

    def _getStrFilterValue(self, query, tabname, attrib, representative, max_length):
        representative = self.__get_minimal_representative_str(attrib, query, representative, tabname)
        output = self.__handle_for_wildcard_char_underscore(attrib, query, representative, tabname)
        self.logger.debug(f"rep: {representative}, handling _: {output}")
        if output == '':
            return output
        output = self.__handle_for_wildcard_char_perc(attrib, max_length, output, query, tabname)
        return output

    def __handle_for_wildcard_char_perc(self, attrib, max_length, output, query, tabname):
        # GET % positions
        index = 0
        representative = copy.deepcopy(output)
        self.logger.debug(representative)
        if len(representative) < max_length:
            output = ""

            while index < len(representative):
                temp = list(representative)
                if temp[index] == 'a':
                    temp.insert(index, 'b')
                else:
                    temp.insert(index, 'a')
                temp = ''.join(temp)
                output = self.__try_with_temp(attrib, output, query, tabname, temp)
                output = output + representative[index]
                index = index + 1

            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)
            output = self.__try_with_temp(attrib, output, query, tabname, temp)
        return output

    def __try_with_temp(self, attrib, output, query, tabname, temp):
        u_query = self.connectionHelper.queries.update_tab_attrib_with_quoted_value(tabname, attrib, temp)
        try:
            self.connectionHelper.execute_sql([u_query], self.logger)
            new_result = self.app.doJob(query)
            if self.app.isQ_result_no_full_nullfree_row(new_result):
                output = output + '%'
        except Exception as e:
            self.logger.debug(e)
            self.connectionHelper.rollback_transaction()
        return output

    def __handle_for_wildcard_char_underscore(self, attrib, query, representative, tabname):
        index = 0
        output = ""
        # currently inverted exclaimaination is being used assuming it will not be in the string
        # GET minimal string with _
        while index < len(representative):

            temp = list(representative)
            if temp[index] == 'a':
                temp[index] = 'b'
            else:
                temp[index] = 'a'
            temp = ''.join(temp)
            u_query = self.connectionHelper.queries.update_tab_attrib_with_quoted_value(tabname, attrib, temp)

            try:
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                if self.app.isQ_result_empty(new_result):
                    temp = copy.deepcopy(representative)
                    temp = temp[:index] + temp[index + 1:]

                    u_query = self.connectionHelper.queries.update_tab_attrib_with_quoted_value(tabname, attrib, temp)
                    try:
                        self.connectionHelper.execute_sql([u_query])
                        new_result = self.app.doJob(query)
                        if self.app.isQ_result_no_full_nullfree_row(new_result):
                            representative = representative[:index] + representative[index + 1:]
                        else:
                            output = output + "_"
                            representative = list(representative)
                            representative[index] = u"\u00A1"
                            representative = ''.join(representative)
                    except Exception as e:
                        self.logger.debug(e)
                        output = output + "_"
                        representative = list(representative)
                        representative[index] = u"\u00A1"
                        representative = ''.join(representative)
                else:
                    output = output + representative[index]
            except Exception as e:
                self.logger.debug(e)
                output = output + representative[index]

            index = index + 1
        return output

    def __get_minimal_representative_str(self, attrib, query, representative, tabname):
        index = 0
        output = ""
        temp = list(representative)
        while index < len(representative):
            temp[index] = ''
            temp_str = ''.join(temp)
            u_query = self.connectionHelper.queries.update_tab_attrib_with_quoted_value(tabname, attrib, temp_str)
            try:
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                if self.app.isQ_result_no_full_nullfree_row(new_result):
                    pass
                else:
                    output = output + representative[index]
                    temp[index] = representative[index]
            except Exception as e:
                self.logger.debug(e)
                output = output + representative[index]
                temp[index] = representative[index]
            index = index + 1
        return output

    def generate_where_clause(self, fp_where):
        for elt in fp_where:
            predicate = self.__generate_where_clause_predicate_str(elt)
            if len(predicate):
                self.where_op = predicate if self.where_op == '' else self.where_op + " and " + predicate

    def generate_from_on_clause(self, edge, fp_on, imp_t1, imp_t2, table1, table2):
        flag_first = True if self._workingCopy.from_op == '' else False
        type_of_join = self.join_map.get((imp_t1, imp_t2))
        join_condition = f"\n\t ON {edge[0][1]}.{edge[0][0]} = {edge[1][1]}.{edge[1][0]}"
        relevant_tables = [table2] if not flag_first else [table1, table2]
        join_part = f"\n{type_of_join} {table2} {join_condition}"
        self.from_op += f" {table1} {join_part}" if flag_first else "" + join_part
        flag_first = False
        for fp in fp_on:
            tables = find_tables_from_predicate(fp)
            if all(tab in relevant_tables for tab in tables):
                predicate = self.__generate_where_clause_predicate_str(fp)
                if len(predicate):
                    self.from_op += "\n\t and " + predicate
        return flag_first

    def __generate_where_clause_predicate_str(self, fp):
        predicate = ''
        if len(fp) == 3:
            predicate = self.get_aoa_string(fp)
        elif len(fp) >= 4:
            predicate = self.formulate_predicate_from_filter(fp)
        return predicate

    def clear_from_where_ops(self):
        self._workingCopy.from_op = ''
        self._workingCopy.where_op = ''

    def get_aoa_string(self, aoa):
        lesser, op, greater = aoa[0], aoa[1], aoa[2]
        tab_attrib = lesser if isinstance(lesser, tuple) else greater
        datatype = self.get_datatype(tab_attrib)
        f_str = ""
        for tup in aoa:
            if isinstance(tup, tuple):
                f_str += f"{tup[0]}.{tup[1]}"
            elif tup in ['<', '<=']:
                f_str += f" {tup} "
            else:
                self.logger.debug(tup)
                f_val = get_format(datatype, tup)
                self.logger.debug(f_val)
                f_str += f_val
        self.logger.debug(f_str)
        return f_str
