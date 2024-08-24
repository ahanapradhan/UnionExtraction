import ast
import copy
import math

from frozenlist._frozenlist import FrozenList

from .dataclass.genPipeline_context import GenPipelineContext
from .projection import get_param_values_external
from ..util.utils import is_number, get_dummy_val_for, get_val_plus_delta, get_format, get_char
from ...src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase, get_boundary_value
from ..util.constants import NUMBER_TYPES
from ...src.util.constants import SUM, AVG, MIN, MAX, COUNT, COUNT_STAR
from ...src.util.constants import min_int_val, max_int_val


def get_k_value_for_number(a, b):
    if a == b:
        k_value = 1
        if a == 2:
            k_value = 2
        agg_array = [SUM, k_value * a + b, AVG, a, MIN, a, MAX, a, COUNT, k_value + 1]
    else:
        constraint_array = [0, a, b, a - 1, b - 1]
        if a != 0:
            constraint_array.append((a - b) / a)
        if a != 1:
            constraint_array.append((1 - b) / (a - 1))
        if (a - 2) ** 2 - (4 * (1 - b)) >= 0:
            constraint_array.append(((a - 2) + math.sqrt((a - 2) ** 2 - (4 * (1 - b)))) / 2)
        k_value = 2
        while k_value in constraint_array:
            k_value = k_value + 1
        avg = round((k_value * a + b) / (k_value + 1), 2)
        agg_array = [SUM, k_value * a + b, AVG, avg, MIN, min(a, b), MAX, max(a, b), COUNT, k_value + 1]
    return k_value, agg_array


def get_k_value(attrib, filter_attrib_dict, groupby_key_flag, tabname, datatype):
    if groupby_key_flag and datatype in NUMBER_TYPES:
        a = b = 3
        k_value = 1
        agg_array = [SUM, k_value * a + b, AVG, a, MIN, a, MAX, a, COUNT, k_value + 1]
    elif (tabname, attrib) in filter_attrib_dict.keys():
        if datatype in NUMBER_TYPES:
            # PRECISION TO BE TAKEN CARE FOR NUMERIC
            a, b = filter_attrib_dict[(tabname, attrib)][0], filter_attrib_dict[(tabname, attrib)][1]
            a = get_boundary_value(a, is_ub=False)
            b = get_boundary_value(b, is_ub=True)
            b = min(a + 1, b)
            if a == 0:  # swap a and b
                a = b
                b = 0
            k_value, agg_array = get_k_value_for_number(a, b)
        elif datatype == 'date':
            date_lb, date_ub = filter_attrib_dict[(tabname, attrib)][0], filter_attrib_dict[(tabname, attrib)][1]
            date_lb = get_boundary_value(date_lb, is_ub=False)
            date_ub = get_boundary_value(date_ub, is_ub=True)
            a = get_format('date', date_lb)
            date_val_plus_1 = get_val_plus_delta('date', date_lb, 1)
            b = get_format('date', min(date_val_plus_1, date_ub))
            k_value = 1
            agg_array = [MIN, min(a, b), MAX, max(a, b)]
            a = ast.literal_eval(a)
            b = ast.literal_eval(b)
        else:
            # string filter attribute
            if '_' in filter_attrib_dict[(tabname, attrib)]:
                a = filter_attrib_dict[(tabname, attrib)].replace('_', 'a')
                b = filter_attrib_dict[(tabname, attrib)].replace('_', 'b')
            else:
                a = filter_attrib_dict[(tabname, attrib)].replace('%', 'a', 1)
                b = filter_attrib_dict[(tabname, attrib)].replace('%', 'b', 1)
            a = a.replace('%', '')
            b = b.replace('%', '')
            k_value = 1
            agg_array = [MIN, min(a, b), MAX, max(a, b)]
    else:
        if datatype == 'date':
            a = get_format('date', get_dummy_val_for('date'))
            b = get_format('date', get_val_plus_delta('date', get_dummy_val_for('date'), 1))
            k_value = 1
            agg_array = [MIN, min(a, b), MAX, max(a, b)]
        elif datatype in NUMBER_TYPES:
            # Combination which gives all different results for aggregation
            a = 5
            b = 8
            k_value = 2
            agg_array = [SUM, 18, AVG, 6, MIN, 5, MAX, 8, COUNT, 3]
        else:
            # String data type
            a = get_char(get_dummy_val_for('char'))
            b = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), 1))
            k_value = 1
            agg_array = [MIN, min(a, b), MAX, max(a, b)]
    # print(tabname, attrib, a, b)
    return a, agg_array, b, k_value


def get_no_of_rows(attrib_list_inner, k_value, key_list, tabname, tabname_inner, result_index, deps):
    same_tab_flag = False
    local_dep = deps[result_index]
    if tabname_inner == tabname:
        no_of_rows = k_value + 1
        same_tab_flag = True
    else:
        no_of_rows = 1
    key_path_flag = False
    for val in attrib_list_inner:
        if val in key_list:
            key_path_flag = True
            break
    if not same_tab_flag and key_path_flag:
        no_of_rows = 2
    return no_of_rows


class Aggregation(GenerationPipeLineBase):
    def __init__(self, connectionHelper,
                 genPipelineCtx: GenPipelineContext,
                 pgao_Ctx):
        super().__init__(connectionHelper, "Aggregation", genPipelineCtx)
        self.global_aggregated_attributes = None
        self.global_projected_attributes = pgao_Ctx.projected_attribs
        self.has_groupby = pgao_Ctx.has_groupby
        self.global_groupby_attributes = pgao_Ctx.group_by_attrib
        self.dependencies = pgao_Ctx.projection_dependencies
        self.solution = pgao_Ctx.projection_solution
        self.param_list = pgao_Ctx.projection_param_list

    def doExtractJob(self, query):
        # AsSUMing NO DISTINCT IN AGGREGATION

        self.global_aggregated_attributes = [(element, '') for element in self.global_projected_attributes]
        if not self.has_groupby:
            return False
        for tabname in self.core_relations:
            attrib_list = copy.deepcopy(self.global_all_attribs[tabname])
            for attrib in attrib_list:
                # check if it is a key attribute
                key_list = next((elt for elt in self.global_join_graph if attrib in elt), [])

                self.logger.debug("Group By Attribs", self.global_groupby_attributes)
                self.logger.debug("Key attribs", key_list)
                tc = False
                for _key in key_list:
                    if _key in self.global_groupby_attributes:
                        tc = True
                if tc:
                    continue
                self.logger.debug("Not groupby", attrib)
                # Attribute Filtering
                if attrib in self.global_groupby_attributes:
                    continue

                result_index_list = []

                for j, dep in enumerate(self.dependencies):
                    for d in dep:
                        if attrib in d and self.global_aggregated_attributes[j][1] == '':
                            result_index_list.append(j)
                            break

                groupby_key_flag = False
                if attrib in self.joined_attribs and attrib in self.global_groupby_attributes:
                    groupby_key_flag = True
                for result_index in result_index_list:
                    datatype = self.get_datatype((tabname, attrib))
                    a, agg_array, b, k_value = get_k_value(attrib, self.filter_attrib_dict,
                                                           groupby_key_flag, tabname, datatype)

                    self.truncate_core_relations()
                    temp_vals = []
                    max_no_of_rows = self.insert_for_inner(a, attrib, b, k_value, key_list, tabname, temp_vals,
                                                           result_index)
                    self.logger.debug(self.dependencies, result_index,
                                      key_list, tabname, temp_vals, result_index)

                    if len(self.solution[result_index]) > 1:
                        self.logger.debug("Temp values", temp_vals)  # FOR DEBUG
                        s = 0
                        mi = max_int_val
                        ma = min_int_val
                        av = 0
                        temp_ar = []
                        local_sol = self.solution[result_index]
                        for ele in self.dependencies[result_index]:
                            local_tabname = ele[0]
                            local_attrib = ele[1]
                            local_attrib_index = self.global_all_attribs[local_tabname].index(local_attrib)
                            vals_sp = temp_vals[self.core_relations.index(local_tabname)]
                            l = []
                            for row in vals_sp:
                                l.append(row[local_attrib_index])
                            temp_ar.append((local_attrib, l))
                        temp_ar = sorted(temp_ar, key=lambda x: x[0])
                        self.logger.debug("Temp Arr", temp_ar)  # FOR DEBUG
                        for t in range(len(temp_ar)):
                            if len(temp_ar[t][1]) < max_no_of_rows:
                                while len(temp_ar[t][1]) < max_no_of_rows:
                                    temp_ar[t][1].append(temp_ar[t][1][0])
                        for _row in range(max_no_of_rows):
                            inter_val = []
                            eqn = 0
                            for j in range(len(self.dependencies[result_index])):
                                inter_val.append(float(temp_ar[j][1][_row]))
                            n = len(self.dependencies[result_index])

                            temp_arr = get_param_values_external(inter_val)
                            self.logger.debug("Coeffs", temp_arr, local_sol)  # FOR DEBUG
                            inter_val = [0 for j in range(len(self.param_list[result_index]))]
                            for j in range(len(self.param_list[result_index])):
                                inter_val[j] = temp_arr[j]
                            inter_val.append(1)
                            self.logger.debug("Intermediate Values of all", inter_val)  # FOR DEBUG
                            for j, val in enumerate(inter_val):
                                eqn += (val * local_sol[j][0])
                            # print("Expression", eqn) # FOR DEBUG
                            s += eqn
                            mi = eqn if eqn < mi else mi
                            ma = eqn if eqn > ma else ma
                        self.logger.debug("no_of_rows ", max_no_of_rows)
                        av = (s / max_no_of_rows)
                        self.logger.debug("Temp Array", temp_ar)
                        self.logger.debug("SUM, AV, MIN, MAX", s, av, mi, ma)
                        agg_array = [SUM, s, AVG, av, MIN, mi, MAX, ma, COUNT, max_no_of_rows]
                    new_result = self.app.doJob(query)
                    self.logger.debug("New Result", new_result)  # FOR DEBUG
                    self.logger.debug("Comaparison", agg_array)  # FOR DEBUG
                    if self.app.isQ_result_empty(new_result):
                        self.logger.error('some error in generating new database. '
                                          'Result is empty. Can not identify aggregation')
                        return False
                    nullfree_rows = self.app.get_all_nullfree_rows(new_result)
                    if len(nullfree_rows) > 1:
                        continue

                    self.analyze(agg_array, self.global_projected_attributes[result_index], nullfree_rows, result_index)

        for i in range(len(self.global_projected_attributes)):
            if self.global_projected_attributes[i] == '':
                self.global_aggregated_attributes[i] = ('', COUNT_STAR)

        return True

    def insert_for_inner(self, a, attrib, b, k_value, key_list, tabname, temp_vals, result_index):
        max_no_of_rows = 0
        # For this table (tabname) and this attribute (attrib), fill all tables now
        for tabname_inner in self.core_relations:
            attrib_list_inner = self.global_all_attribs[tabname_inner]
            insert_rows = []
            no_of_rows = get_no_of_rows(attrib_list_inner, k_value, key_list, tabname, tabname_inner, result_index,
                                        self.dependencies)

            if no_of_rows > max_no_of_rows:
                max_no_of_rows = no_of_rows

            self.logger.debug("tabname ", tabname, " tabname_inner ", tabname_inner, " no_of_rows ", no_of_rows)
            attrib_list_str = ",".join(attrib_list_inner)
            att_order = f"({attrib_list_str})"

            for k in range(no_of_rows):
                insert_values = []

                for attrib_inner in attrib_list_inner:
                    datatype = self.get_datatype((tabname_inner, attrib_inner))
                    if (attrib_inner == attrib or attrib_inner in key_list) and k == no_of_rows - 1:
                        insert_values.append(b)
                    elif attrib_inner == attrib or attrib_inner in key_list:
                        insert_values.append(a)
                    elif datatype == 'date':
                        # check for filter
                        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
                            date_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
                            date_val = get_boundary_value(date_val, is_ub=False)
                        else:
                            date_val = get_val_plus_delta('date', get_dummy_val_for('date'), 2)
                        insert_values.append(ast.literal_eval(get_format('date', date_val)))
                    elif datatype in NUMBER_TYPES:
                        # check for filter
                        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
                            number_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
                            number_val = get_boundary_value(number_val, is_ub=False)
                        else:
                            number_val = get_dummy_val_for('int')
                        insert_values.append(number_val)
                    else:
                        # check for filter
                        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
                            attrib_val = self.get_s_val_for_textType(attrib_inner, tabname_inner)
                            plus_val = attrib_val.replace('%', '')
                        else:
                            plus_val = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), 2))
                        insert_values.append(plus_val)
                    self.logger.debug(tabname_inner, attrib_inner, datatype, insert_values[-1])
                insert_rows.append(tuple(insert_values))

            self.logger.debug("Attribute Ordering: ", att_order)  # FOR DEBUG
            self.logger.debug("Rows: ", insert_rows)  # FOR DEBUG
            temp_vals.append(insert_rows)
            self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)
        return max_no_of_rows

    def analyze(self, agg_array, attrib, new_result, result_index):
        self.logger.debug("analyze")
        new_result = list(new_result[0])
        new_result = [x.strip() for x in new_result]
        check_value = round(float(new_result[result_index]), 2) if is_number(new_result[result_index]) \
            else str(new_result[result_index])
        agg_array = [round(x, 2) if isinstance(x, float) else x for x in agg_array]
        j = 0
        while j < len(agg_array) - 1:
            self.logger.debug(str(attrib), " ", agg_array[j])
            self.logger.debug(check_value, " ", agg_array[j + 1])
            if check_value == agg_array[j + 1]:
                self.global_aggregated_attributes[result_index] = (str(attrib), agg_array[j])
                break
            j = j + 2
