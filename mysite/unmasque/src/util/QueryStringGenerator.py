import copy

from mysite.unmasque.src.core.executable import Executable
from mysite.unmasque.src.core.factory.ExecutableFactory import ExecutableFactory
from mysite.unmasque.src.util.Log import Log
from mysite.unmasque.src.util.constants import COUNT, SUM, max_str_len
from mysite.unmasque.src.util.utils import get_format, get_datatype_of_val


class QueryString:

    def __init__(self):
        self._select_op = None
        self._from_op = None
        self._where_op = None
        self._group_by_op = None
        self._order_by_op = None
        self._limit_op = None

    @property
    def select_op(self):
        return self._select_op

    @select_op.setter
    def select_op(self, value):
        self._select_op = value

    @property
    def from_op(self):
        return self._from_op

    @from_op.setter
    def from_op(self, value):
        self._from_op = value

    @property
    def where_op(self):
        return self._where_op

    @where_op.setter
    def where_op(self, value):
        self._where_op = value

    @property
    def group_by_op(self):
        return self._group_by_op

    @group_by_op.setter
    def group_by_op(self, value):
        self._group_by_op = value

    @property
    def order_by_op(self):
        return self._order_by_op

    @order_by_op.setter
    def order_by_op(self, value):
        self._order_by_op = value

    @property
    def limit_op(self):
        return self._limit_op

    @limit_op.setter
    def limit_op(self, value):
        self._limit_op = value

    def assembleQuery(self):
        output = f"Select {self.select_op}\n From {self.from_op}"
        if self.where_op is not None:
            output = f"{output} \n Where {self.where_op}"
        if self.group_by_op is not None:
            output = f"{output} \n Group By {self.group_by_op}"
        if self.order_by_op is not None:
            output = f"{output} \n Order By {self.order_by_op}"
        if self.limit_op is not None:
            output = f"{output} \n Limit {self.limit_op}"
        output = f"{output};"
        return output


class QueryDetails:
    def __init__(self):
        self.core_relations = []

        self.eq_join_predicates = []
        self.join_graph = []
        self.filter_in_predicates = []
        self.filter_predicates = []
        self.aoa_less_thans = []
        self.aoa_predicates = []
        self.join_edges = []

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
        self.join_graph = other.join_graph
        self.filter_in_predicates = other.filter_in_predicates
        self.filter_predicates = other.filter_predicates
        self.aoa_less_thans = other.aoa_less_thans
        self.aoa_predicates = other.aoa_predicates
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


class QueryStringGenerator:
    def __init__(self, connectionHelper):
        self.connectionHelper = connectionHelper
        exeFactory = ExecutableFactory()
        self.app = exeFactory.create_exe(self.connectionHelper)
        self.__get_datatype = None
        self._queries = {}
        self._workingCopy = QueryDetails()
        self.logger = Log("Query String Generator", connectionHelper.config.log_level)

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
        return NotImplementedError

    @from_clause.setter
    def from_clause(self, core_relations):
        self._workingCopy.core_relations = core_relations
        self._workingCopy.core_relations.sort()

    @property
    def equi_join(self):
        return NotImplementedError

    @equi_join.setter
    def equi_join(self, aoa):
        self._workingCopy.eq_join_predicates = aoa.algebraic_eq_predicates

    @property
    def orderby(self):
        return NotImplementedError

    @orderby.setter
    def orderby(self, ob_obj):
        self._workingCopy.order_by_op = ob_obj.orderBy_string

    @property
    def limit(self):
        return NotImplementedError

    @limit.setter
    def limit(self, lm_obj):
        self._workingCopy.limit_op = str(lm_obj.limit) if lm_obj.limit is not None else ''

    @property
    def projection(self):
        return NotImplementedError

    @projection.setter
    def projection(self, pj_obj):
        self._workingCopy.global_key_attributes = pj_obj.joined_attribs
        self._workingCopy.projection_names = pj_obj.projection_names

    @property
    def aggregate(self):
        return NotImplementedError

    @aggregate.setter
    def aggregate(self, agg_obj):
        self._workingCopy.global_aggregated_attributes = agg_obj.global_aggregated_attributes
        self._workingCopy.global_groupby_attributes = agg_obj.global_groupby_attributes
        self._workingCopy.global_projected_attributes = agg_obj.global_projected_attributes

    @property
    def where_clause_remnants(self):
        return NotImplementedError

    @where_clause_remnants.setter
    def where_clause_remnants(self, delivery):
        self._workingCopy.aoa_predicates = delivery.global_aoa_le_predicates
        self._workingCopy.aoa_less_thans = delivery.global_aoa_l_predicates
        self._workingCopy.filter_predicates = delivery.global_filter_predicates

    @property
    def all_arithmetic_filters(self):
        preds = self._workingCopy.filter_predicates + self._workingCopy.filter_in_predicates
        return preds

    @all_arithmetic_filters.setter
    def all_arithmetic_filters(self, value):
        raise NotImplementedError

    @property
    def join_edges(self):
        return self._workingCopy.join_edges

    @join_edges.setter
    def join_edges(self, value):
        self._workingCopy.join_edges = value
        self._workingCopy.eq_join_predicates.clear()  # when join edges are assigned directly, old equi join predicates are obsolete

    def updateExtractedQueryWithNEPVal(self, query, val):
        for elt in val:
            tab, attrib, op, neg_val = elt[0], elt[1], elt[2], elt[3]
            datatype = self.get_datatype((tab, attrib))
            format_val = get_format(datatype, neg_val)
            if datatype == 'str':
                output = self._getStrFilterValue(query, elt[0], elt[1], elt[3], max_str_len)
                self.logger.debug(output)
                if '%' in output or '_' in output:
                    predicate = f"{tab}.{attrib} NOT LIKE '{str(output)}' "
                    self._remove_exact_NE_string_predicate(elt)
                else:
                    predicate = f"{tab}.{attrib} {str(op)} \'{str(output)}\' "
            else:
                predicate = f"{tab}.{attrib} {str(op)} {format_val}"

            self._workingCopy.add_to_where_op(predicate)

        Q_E = self.write_query()
        return Q_E

    def __generate_where_clause(self, all_ors=None) -> str:
        predicates = []
        if not len(self._workingCopy.join_edges):
            self.__generate_algebraice_eualities(predicates)
        else:
            predicates.extend(self._workingCopy.join_edges)
        self.__generate_algebraic_inequalities(predicates)

        if self.connectionHelper.config.detect_or:
            self.__generate_arithmetic_conjunctive_disjunctions(all_ors, predicates)
        else:
            self.__generate_arithmetic_pure_conjunctions(predicates)

        where_clause = "\n and ".join(predicates)
        self.logger.debug(where_clause)
        return where_clause

    def generate_query_string(self, all_ors=None):
        self._workingCopy.from_op = ", ".join(self._workingCopy.core_relations)
        self._workingCopy.where_op = self.__generate_where_clause(all_ors)
        self.__generate_group_by_clause()
        self.__generate_select_clause()
        eq = self.write_query()
        return eq

    def rewrite_query(self, core_relations, ed_join_edges, filter_predicates, gaol=True):
        last_queryDetails = QueryDetails()
        last_queryDetails.makeCopy(self._workingCopy)
        self.from_clause = core_relations
        self.join_edges = ed_join_edges
        self._workingCopy.filter_predicates = filter_predicates
        eq = self.write_query(gaol)
        return eq

    def create_new_query(self):
        lastQueryDetails = QueryDetails()
        lastQueryDetails.makeCopy(self._workingCopy)
        self.write_query()  # take backup of current working copy

    def write_query(self, gaol=True) -> str:

        self.logger.debug(self._workingCopy.select_op)
        self.logger.debug(self._workingCopy.from_op)
        self.logger.debug(self._workingCopy.where_op)
        self.logger.debug(self._workingCopy.group_by_op)
        self.logger.debug(self._workingCopy.order_by_op)
        self.logger.debug(self._workingCopy.limit_op)

        query = QueryString()
        self.__generate_SPJ_string(query)
        if gaol:
            self.__generate_GAOL_string(query)
        query_string = query.assembleQuery()
        key = hash(query_string)
        if key not in self._queries:
            self._queries[key] = (query, copy.deepcopy(self._workingCopy))
        return query_string

    def __generate_SPJ_string(self, query):
        query.select_op = self._workingCopy.select_op
        query.from_op = self._workingCopy.from_op
        query.where_op = self._workingCopy.where_op if self._workingCopy.where_op != '' else None

    def __generate_GAOL_string(self, query):
        query.group_by_op = self._workingCopy.group_by_op if self._workingCopy.group_by_op != '' else None
        query.order_by_op = self._workingCopy.order_by_op if self._workingCopy.order_by_op != '' else None
        query.limit_op = self._workingCopy.limit_op if self._workingCopy.limit_op != '' else None

    def formulate_predicate_from_filter(self, elt):
        tab, attrib, op, lb, ub = elt[0], elt[1], str(elt[2]).strip().lower(), elt[3], elt[4]
        datatype = self.get_datatype((tab, attrib))
        f_lb = f"({', '.join(lb)})" if isinstance(lb, list) else get_format(datatype, lb)
        f_ub = f"({', '.join(ub)})" if isinstance(ub, list) else get_format(datatype, ub)
        if op == 'range':
            predicate = f"{tab}.{attrib} between {f_lb} and {f_ub}"
        elif op == '>=':
            predicate = f"{tab}.{attrib} {op} {f_lb}"
        elif op in ['<=', '=']:
            predicate = f"{tab}.{attrib} {op} {f_ub}"
        elif 'equal' in op or 'like' in op or '-' in op:
            predicate = f"{tab}.{attrib} {str(op.replace('equal', '='))} {f_ub}"
        elif op == 'in':
            predicate = f"{tab}.{attrib} IN {f_ub}"
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
        for aoa in self._workingCopy.aoa_predicates:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" <= ".join(pred))
        for aoa in self._workingCopy.aoa_less_thans:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" < ".join(pred))

    def __generate_arithmetic_pure_conjunctions(self, predicates):
        for a_eq in self._workingCopy.filter_predicates:
            pred = self.formulate_predicate_from_filter(a_eq)
            predicates.append(pred)

    def __generate_arithmetic_conjunctive_disjunctions(self, all_ors, predicates):
        for p in all_ors:
            non_empty_indices = [i for i, t_a in enumerate(p) if t_a]
            tab_attribs = [(p[i][0], p[i][1]) for i in non_empty_indices]
            ops = [p[i][2] for i in non_empty_indices]
            datatypes = [self.get_datatype(tab_attribs[i]) for i in non_empty_indices]
            values = [get_format(datatypes[i], p[i][3]) for i in non_empty_indices]
            values.sort()
            uniq_tab_attribs = set(tab_attribs)
            if len(uniq_tab_attribs) == 1 and all(op in ['equal', '='] for op in ops):
                tab, attrib = next(iter(uniq_tab_attribs))
                in_pred = self.__adjust_for_in_predicates(attrib, tab, values)
                one_pred = self.formulate_predicate_from_filter(in_pred)
            else:
                pred_str, preds = "", []
                for i in non_empty_indices:
                    pred_str = self.formulate_predicate_from_filter(p[i])
                    preds.append(pred_str)
                one_pred = " OR ".join(preds)
            predicates.append(one_pred)

    def __adjust_for_in_predicates(self, attrib, tab, values):
        in_pred = [tab, attrib, 'IN', values, values] if len(values) > 1 else [tab, attrib, '=', values, values]
        self._workingCopy.filter_in_predicates.append(tuple(in_pred))
        remove_eq_filter_predicate = []
        for eq_pred in self._workingCopy.filter_predicates:
            if eq_pred[0] == tab and eq_pred[1] == attrib and eq_pred[2] in ['equal', '=']:
                remove_eq_filter_predicate.append(eq_pred)
        for t_r in remove_eq_filter_predicate:
            self._workingCopy.filter_predicates.remove(t_r)
        return tuple(in_pred)

    def __optimize_group_by_attributes(self):
        for i in range(len(self._workingCopy.global_projected_attributes)):
            attrib = self._workingCopy.global_projected_attributes[i]
            if (attrib in self._workingCopy.global_key_attributes
                    and attrib in self._workingCopy.global_groupby_attributes):
                if not (SUM in self._workingCopy.global_aggregated_attributes[i][1] or COUNT in
                        self._workingCopy.global_aggregated_attributes[i][1]):
                    self._workingCopy.global_aggregated_attributes[i] = (
                        self._workingCopy.global_aggregated_attributes[i][0], '')
        temp_list = copy.deepcopy(self._workingCopy.global_groupby_attributes)
        for attrib in temp_list:
            if attrib not in self._workingCopy.global_projected_attributes:
                try:
                    self._workingCopy.global_groupby_attributes.remove(attrib)
                except:
                    pass
                continue
            remove_flag = True
            for elt in self._workingCopy.global_aggregated_attributes:
                if elt[0] == attrib and (not (SUM in elt[1] or COUNT in elt[1])):
                    remove_flag = False
                    break
            if remove_flag:
                try:
                    self._workingCopy.global_groupby_attributes.remove(attrib)
                except:
                    pass

    def __generate_select_clause(self):
        for i in range(len(self._workingCopy.global_projected_attributes)):
            elt = self._workingCopy.global_projected_attributes[i]
            if self._workingCopy.global_aggregated_attributes[i][1] != '':
                if COUNT in self._workingCopy.global_aggregated_attributes[i][1]:
                    elt = self._workingCopy.global_aggregated_attributes[i][1]
                else:
                    elt = self._workingCopy.global_aggregated_attributes[i][1] + '(' + elt + ')'

            if elt != self._workingCopy.projection_names[i] and self._workingCopy.projection_names[i] != '':
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
            attrib_index = where_parts.index(elt[1])

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
                output = self.try_with_temp(attrib, output, query, tabname, temp)
                output = output + representative[index]
                index = index + 1

            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)
            output = self.try_with_temp(attrib, output, query, tabname, temp)
        return output

    def try_with_temp(self, attrib, output, query, tabname, temp):
        u_query = self.connectionHelper.queries.update_tab_attrib_with_quoted_value(tabname, attrib, temp)
        try:
            self.connectionHelper.execute_sql([u_query], self.logger)
            new_result = self.app.doJob(query)
            if self.app.isQ_result_empty(new_result):
                output = output + '%'
        except Exception as e:
            self.logger.debug(e)
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
                        if self.app.isQ_result_empty(new_result):
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
                if self.app.isQ_result_empty(new_result):
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


def add_pred_for(aoa_l, pred):
    if isinstance(aoa_l, list) or isinstance(aoa_l, tuple):
        pred.append(f"{aoa_l[0]}.{aoa_l[1]}")
    else:
        pred.append(get_format(get_datatype_of_val(aoa_l), aoa_l))
    return aoa_l
