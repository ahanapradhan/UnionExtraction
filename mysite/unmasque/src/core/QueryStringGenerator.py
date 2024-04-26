import copy

from ..util.constants import COUNT, SUM, max_str_len
from ...src.core.abstract.AppExtractorBase import AppExtractorBase


def get_exact_NE_string_predicate(elt, output):
    return f"{elt[0]}.{elt[1]} {str(elt[2])} \'{str(output)}\' "


class QueryStringGenerator(AppExtractorBase):

    def doActualJob(self, args=None):
        pass

    def extract_params_from_args(self, args):
        pass

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Query String Generator")
        self.select_op = ''
        self.from_op = ''
        self.where_op = ''
        self.group_by_op = ''
        self.order_by_op = ''
        self.limit_op = None

    def generate_query_string(self, core_relations, pj, gb, agg, ob, lm, aoa):
        relations = copy.deepcopy(core_relations)
        relations.sort()
        self.from_op = ", ".join(relations)
        self.where_op = aoa.where_clause
        eq = self.__refine_Query1(pj.joined_attribs, pj, gb, agg, ob, lm)
        return eq

    def assembleQuery(self):
        output = f"Select {self.select_op}\n From {self.from_op}"
        if self.where_op != '':
            output = f"{output} \n Where {self.where_op}"
        if self.group_by_op != '':
            output = f"{output} \n Group By {self.group_by_op}"
        if self.order_by_op != '':
            output = f"{output} \n Order By {self.order_by_op}"
        if self.limit_op is not None:
            output = f"{output} \n Limit {self.limit_op}"
        output = f"{output};"
        return output

    def __refine_Query1(self, global_key_attributes, pj, gb, agg, ob, lm):
        self.logger.debug("inside:   reveal_proc_support.refine_Query")
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

        # UPDATE OUTPUTS
        first_occur = True
        self.group_by_op = ''
        for i in range(len(agg.global_groupby_attributes)):
            elt = agg.global_groupby_attributes[i]
            if first_occur:
                self.group_by_op = elt
                first_occur = False
            else:
                self.group_by_op = self.group_by_op + ", " + elt
        first_occur = True
        for i in range(len(agg.global_projected_attributes)):
            elt = agg.global_projected_attributes[i]
            if agg.global_aggregated_attributes[i][1] != '':
                if COUNT in agg.global_aggregated_attributes[i][1]:
                    elt = agg.global_aggregated_attributes[i][1]
                else:
                    elt = agg.global_aggregated_attributes[i][1] + '(' + elt + ')'

            if elt != pj.projection_names[i] and pj.projection_names[i] != '':
                elt = elt + ' as ' + pj.projection_names[i]
            if first_occur:
                self.select_op = elt
                first_occur = False
            else:
                self.select_op = self.select_op + ", " + elt

        self.order_by_op = ob.orderBy_string
        if lm.limit is not None:
            self.limit_op = str(lm.limit)
        eq = self.assembleQuery()
        return eq

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

            if self.where_op and predicate not in self.where_op:
                self.where_op = f'{self.where_op} and {predicate}'
            else:
                self.where_op = predicate

        Q_E = self.assembleQuery()
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
