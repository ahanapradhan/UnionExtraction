import copy

from ..util.aoa_utils import add_pred_for
from ..util.constants import COUNT, SUM, max_str_len
from ..util.utils import get_format
from ...src.core.abstract.AppExtractorBase import AppExtractorBase


def get_exact_NE_string_predicate(elt, output):
    return f"{elt[0]}.{elt[1]} {str(elt[2])} \'{str(output)}\' "


class QueryString:

    def __init__(self):
        self._select_op = ''
        self._from_op = ''
        self._where_op = ''
        self._group_by_op = ''
        self._order_by_op = ''
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


class QueryStringGenerator(AppExtractorBase):

    def doActualJob(self, args=None):
        pass

    def extract_params_from_args(self, args):
        pass

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Query String Generator")
        self.eq_join_predicates = None
        self.join_graph = None
        self.filter_in_predicates = []
        self.filter_predicates = None
        self.aoa_less_thans = None
        self.aoa_predicates = None

        self.get_datatype = None

        self.select_op = ''
        self.from_op = ''
        self.where_op = ''
        self.group_by_op = ''
        self.order_by_op = ''
        self.limit_op = None

    def reset(self):
        self.eq_join_predicates = None
        self.join_graph = None
        self.filter_in_predicates = []
        self.filter_predicates = None
        self.aoa_less_thans = None
        self.aoa_predicates = None

        self.get_datatype = None

        self.select_op = ''
        self.from_op = ''
        self.where_op = ''
        self.group_by_op = ''
        self.order_by_op = ''
        self.limit_op = None

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

    def set_aoa_details(self, delivery):
        self.get_datatype = delivery.get_datatype
        self.aoa_predicates = delivery.global_aoa_le_predicates
        self.aoa_less_thans = delivery.global_aoa_l_predicates
        self.filter_predicates = delivery.global_filter_predicates

    def __generate_algebraice_eualities(self, predicates):
        for eq_join in self.eq_join_predicates:
            self.logger.debug(f"Creating join clause for {eq_join}")
            join_edge = list(f"{item[0]}.{item[1]}" for item in eq_join if len(item) == 2)
            join_edge.sort()
            predicates.extend(f"{join_edge[i]} = {join_edge[i + 1]}" for i in range(len(join_edge) - 1))
        self.logger.debug(predicates)

    def __generate_algebraic_inequalities(self, predicates):
        for aoa in self.aoa_predicates:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" <= ".join(pred))
        for aoa in self.aoa_less_thans:
            pred = []
            add_pred_for(aoa[0], pred)
            add_pred_for(aoa[1], pred)
            predicates.append(" < ".join(pred))

    def __generate_arithmetic_pure_conjunctions(self, predicates):
        for a_eq in self.filter_predicates:
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
        self.filter_in_predicates.append(tuple(in_pred))
        remove_eq_filter_predicate = []
        for eq_pred in self.filter_predicates:
            if eq_pred[0] == tab and eq_pred[1] == attrib and eq_pred[2] in ['equal', '=']:
                remove_eq_filter_predicate.append(eq_pred)
        for t_r in remove_eq_filter_predicate:
            self.filter_predicates.remove(t_r)
        return tuple(in_pred)

    def generate_where_clause(self, all_ors=None) -> str:
        predicates = []
        self.__generate_algebraice_eualities(predicates)
        self.__generate_algebraic_inequalities(predicates)

        if self.connectionHelper.config.detect_or:
            self.__generate_arithmetic_conjunctive_disjunctions(all_ors, predicates)
        else:
            self.__generate_arithmetic_pure_conjunctions(predicates)

        where_clause = "\n and ".join(predicates)
        self.logger.debug(where_clause)
        return where_clause

    def generate_query_string(self, core_relations, eq_join_preds, pj, agg, ob, lm, all_ors):
        self.eq_join_predicates = eq_join_preds
        relations = copy.deepcopy(core_relations)
        relations.sort()
        self.from_op = ", ".join(relations)
        self.where_op = self.generate_where_clause(all_ors)
        self.generate_group_by_clause(agg, pj.joined_attribs)
        self.generate_select_clause(agg, pj)
        self.order_by_op = ob.orderBy_string
        self.limit_op = str(lm.limit) if lm.limit is not None else ''
        eq = self.generate_query()
        return eq

    def generate_query(self):
        query = QueryString()
        query.select_op = self.select_op
        query.from_op = self.from_op
        query.where_op = self.where_op if self.where_op != '' else None
        query.group_by_op = self.group_by_op if self.group_by_op != '' else None
        query.order_by_op = self.order_by_op if self.order_by_op != '' else None
        query.limit_op = self.limit_op if self.limit_op != '' else None
        return query.assembleQuery()

    def generate_select_clause(self, agg, pj):
        for i in range(len(agg.global_projected_attributes)):
            elt = agg.global_projected_attributes[i]
            if agg.global_aggregated_attributes[i][1] != '':
                if COUNT in agg.global_aggregated_attributes[i][1]:
                    elt = agg.global_aggregated_attributes[i][1]
                else:
                    elt = agg.global_aggregated_attributes[i][1] + '(' + elt + ')'

            if elt != pj.projection_names[i] and pj.projection_names[i] != '':
                elt = elt + ' as ' + pj.projection_names[i]
            self.select_op = elt if not i else f'{self.select_op}, {elt}'

    def generate_group_by_clause(self, agg, global_key_attributes):
        for i in range(len(agg.global_projected_attributes)):
            attrib = agg.global_projected_attributes[i]
            if attrib in global_key_attributes and attrib in agg.global_groupby_attributes:
                if not (SUM in agg.global_aggregated_attributes[i][1] or COUNT in
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
                if elt[0] == attrib and (not (SUM in elt[1] or COUNT in elt[1])):
                    remove_flag = False
                    break
            if remove_flag:
                try:
                    agg.global_groupby_attributes.remove(attrib)
                except:
                    pass
        for i in range(len(agg.global_groupby_attributes)):
            elt = agg.global_groupby_attributes[i]
            # UPDATE OUTPUTS
            self.group_by_op = elt if not i else f'{self.group_by_op}, {elt}'

    def updateExtractedQueryWithNEPVal(self, query, val):
        for elt in val:
            if '-' in str(elt[3]):
                predicate = get_exact_NE_string_predicate(elt, elt[3])

            elif isinstance(elt[3], str):
                output = self.getStrFilterValue(query, elt[0], elt[1], elt[3], max_str_len)
                if '%' in output or '_' in output:
                    predicate = f"{elt[0]}.{elt[1]} NOT LIKE '{str(output)}' "
                    self.remove_exact_NE_string_predicate(elt)
                else:
                    predicate = get_exact_NE_string_predicate(elt, output)
            else:
                predicate = f"{elt[0]}.{elt[1]} {str(elt[2])} {str(elt[3])}"

            if self.where_op != '' and predicate not in self.where_op:
                self.where_op = f'{self.where_op} and {predicate}'
            elif self.where_op == '':
                self.where_op = predicate
            else:
                pass

        Q_E = self.generate_query()
        return Q_E

    def remove_exact_NE_string_predicate(self, elt):
        while elt[1] in self.where_op:
            where_parts = self.where_op.split()
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
            self.where_op = " ".join(where_parts)

    def getStrFilterValue(self, query, tabname, attrib, representative, max_length):
        representative = self.__get_minimal_representative_str(attrib, query, representative, tabname)
        output = self.__handle_for_wildcard_char_underscore(attrib, query, representative, tabname)
        if output == '':
            return output
        output = self.__handle_for_wildcard_char_perc(attrib, max_length, output, query, tabname)
        return output

    def __handle_for_wildcard_char_perc(self, attrib, max_length, output, query, tabname):
        # GET % positions
        index = 0
        representative = copy.deepcopy(output)
        if len(representative) < max_length:
            output = ""

            while index < len(representative):

                temp = list(representative)
                if temp[index] == 'a':
                    temp.insert(index, 'b')
                else:
                    temp.insert(index, 'a')
                temp = ''.join(temp)
                u_query = f"update {tabname} set {attrib} = '{temp}';"

                try:
                    self.connectionHelper.execute_sql([u_query])
                    new_result = self.app.doJob(query)
                    if len(new_result) <= 1:
                        output = output + '%'
                except Exception as e:
                    print(e)

                output = output + representative[index]
                index = index + 1

            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)
            u_query = f"update {tabname} set {attrib} = '{temp}';"

            try:
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                if len(new_result) <= 1:
                    output = output + '%'
            except Exception as e:
                print(e)
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
            u_query = f"update {tabname} set {attrib} = '{temp}';"

            try:
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                if len(new_result) <= 1:
                    temp = copy.deepcopy(representative)
                    temp = temp[:index] + temp[index + 1:]

                    u_query = f"update {tabname} set {attrib} = '{temp}';"
                    try:
                        self.connectionHelper.execute_sql([u_query])
                        new_result = self.app.doJob(query)
                        if len(new_result) <= 1:
                            representative = representative[:index] + representative[index + 1:]
                        else:
                            output = output + "_"
                            representative = list(representative)
                            representative[index] = u"\u00A1"
                            representative = ''.join(representative)
                    except Exception as e:
                        print(e)
                        output = output + "_"
                        representative = list(representative)
                        representative[index] = u"\u00A1"
                        representative = ''.join(representative)
                else:
                    output = output + representative[index]
            except Exception as e:
                print(e)
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
            u_query = f"update {tabname} set {attrib} = '{temp_str}';"

            try:
                self.connectionHelper.execute_sql([u_query])
                new_result = self.app.doJob(query)
                if len(new_result) <= 1:
                    pass
                else:
                    output = output + representative[index]
                    temp[index] = representative[index]

            except Exception as e:
                print(e)
                output = output + representative[index]
                temp[index] = representative[index]

            index = index + 1
        return output
