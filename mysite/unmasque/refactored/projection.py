import ast
import copy
import datetime
import numpy as np
import random

from ..src.util import constants
from ..refactored.abstract.AfterWhereClauseExtractorBase import AfterWhereClauseBase
from ..refactored.util.utils import is_number, isQ_result_empty, get_unused_dummy_val, get_format, \
    get_val_plus_delta, get_char, get_dummy_val_for


def cover_special_chars(curr_str, value_used):
    dummy_char = get_unused_dummy_val('char', value_used)
    curr_str = curr_str.replace('_', get_char(dummy_char))
    dummy_char = get_unused_dummy_val('char', value_used)
    curr_str = curr_str.replace('%', get_char(dummy_char), 1)
    curr_str = curr_str.replace('%', '')
    return curr_str


def construct_value_used_for_filtered_attribs(attrib_types_dict, newfilterList, value_used):
    curr_attrib = [newfilterList[0]]
    index = 1
    while index < len(newfilterList) and curr_attrib[0][2] != '=' and curr_attrib[0][2] != 'equal':
        curr_attrib[0] = newfilterList[index]
        index = index + 1
    if 'char' in attrib_types_dict[(curr_attrib[0][0], curr_attrib[0][1])] \
            or \
            'text' in attrib_types_dict[(curr_attrib[0][0], curr_attrib[0][1])]:
        curr_value = cover_special_chars(curr_attrib[0][3], value_used)
    elif 'date' in attrib_types_dict[(curr_attrib[0][0], curr_attrib[0][1])] or 'numeric' in attrib_types_dict[
        (curr_attrib[0][0], curr_attrib[0][1])]:
        curr_value = curr_attrib[0][3]
    else:
        curr_value = int(curr_attrib[0][3])
    value_used = [curr_attrib[0][1], curr_value]
    return curr_attrib, curr_value, value_used


def add_value_used_for_one_filtered_attrib(attrib_types_dict,
                                           curr_attrib, curr_value,
                                           entry, value_used):
    if entry == curr_attrib[0]:
        return
    value_used.append(entry[1])
    if 'int' in attrib_types_dict[(entry[0], entry[1])] or 'numeric' in attrib_types_dict[(entry[0], entry[1])]:
        # indicates integer type attribute
        value_used.append(0)
        for i in range(int(entry[3]), int(entry[4]) + 1):
            value_used[-1] = i
            if i != curr_value:
                break
        if value_used[-1] == curr_value:
            curr_attrib.append(entry)
    elif 'date' in attrib_types_dict[(entry[0], entry[1])]:
        value_used.append(entry[3])
        for i in range(int((entry[4] - entry[3]).days)):
            value_used[-1] = get_val_plus_delta('date', entry[3], i)
            if value_used[-1] == curr_value:
                curr_attrib.append(entry)
            else:
                break
    elif 'char' in attrib_types_dict[(entry[0], entry[1])] or 'text' in attrib_types_dict[(entry[0], entry[1])]:
        # character type attribute
        curr_str = entry[3]
        value_used.append(curr_str)
        while '_' in curr_str or '%' in curr_str:
            curr_str = cover_special_chars(curr_str, value_used)
            if curr_str != curr_value:
                value_used[-1]
                break
            curr_str = entry[3]
        if value_used[-1] == curr_value:
            curr_attrib.append(entry)


class Projection(AfterWhereClauseBase):
    def __init__(self, connectionHelper,
                 global_attrib_types,
                 core_relations,
                 filter_predicates,
                 join_graph,
                 global_all_attribs):
        super().__init__(connectionHelper, "Projection",
                         core_relations,
                         global_all_attribs,
                         global_attrib_types,
                         join_graph,
                         filter_predicates)
        self.projection_names = None
        self.projected_attribs = None
        self.dependencies = None
        self.solution = None
        self.param_list = []

    def construct_values_used(self, attrib_types_dict):
        vu = []
        # Identifying projected attributs with no filter
        for pred in self.global_filter_predicates:
            vu.append(pred[1])
            if 'char' in attrib_types_dict[(pred[0], pred[1])] or 'text' in attrib_types_dict[
                (pred[0], pred[1])]:
                vu.append(pred[3].replace('%', ''))
            else:
                vu.append(pred[3])
        return vu

    def construct_values_for_attribs(self, value_used, attrib_types_dict):
        for elt in self.global_join_graph:
            dummy_int = get_unused_dummy_val('int', value_used)
            for val in elt:
                value_used.append(val)
                value_used.append(dummy_int)
        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            attrib_list = self.global_all_attribs[i]
            insert_values = []
            att_order = '('
            for attrib in attrib_list:
                att_order = att_order + attrib + ","
                if attrib in value_used:
                    if 'int' in attrib_types_dict[(tabname, attrib)] \
                            or \
                            'numeric' in attrib_types_dict[(tabname, attrib)]:
                        insert_values.append(value_used[value_used.index(attrib) + 1])
                    elif 'date' in attrib_types_dict[(tabname, attrib)]:
                        date_val = value_used[value_used.index(attrib) + 1]
                        date_insert = get_format('date', date_val)
                        insert_values.append(ast.literal_eval(date_insert))
                    else:
                        insert_values.append(str(value_used[value_used.index(attrib) + 1]))

                else:
                    value_used.append(attrib)
                    if 'int' in attrib_types_dict[(tabname, attrib)] \
                            or \
                            'numeric' in attrib_types_dict[(tabname, attrib)]:
                        dummy_int = get_unused_dummy_val('int', value_used)
                        insert_values.append(dummy_int)
                        value_used.append(dummy_int)
                    elif 'date' in attrib_types_dict[(tabname, attrib)]:
                        dummy_date = get_unused_dummy_val('date', value_used)
                        val = ast.literal_eval(get_format('date', dummy_date))
                        insert_values.append(val)
                        value_used.append(val)
                    elif 'boolean' in attrib_types_dict[(tabname, attrib)]:
                        insert_values.append(constants.dummy_boolean)
                        value_used.append(str(constants.dummy_boolean))
                    elif 'bit varying' in attrib_types_dict[(tabname, attrib)]:
                        value_used.append(attrib)
                        insert_values.append(constants.dummy_varbit)
                        value_used.append(str(constants.dummy_varbit))
                    else:
                        dummy_char = get_unused_dummy_val('char', value_used)
                        dummy = get_char(dummy_char)
                        insert_values.append(dummy)
                        value_used.append(dummy)

            insert_values = tuple(insert_values)
            self.insert_attrib_vals_into_table(att_order, attrib_list, [insert_values], tabname)

        value_used = [str(val) for val in value_used]
        return value_used

    def doExtractJob(self, query, attrib_types_dict, filter_attrib_dict):
        projected_attrib, projection_names, value_used, check = \
            self.find_projection_on_unfiltered_attribs(attrib_types_dict, query)
        if not check:
            return False

        check = self.find_projection_on_filtered_attribs(attrib_types_dict, projected_attrib, query, value_used)
        if not check:
            return False

        #projection_dep = [[] for i in projected_attrib]
        projection_dep = self.find_dependencies_on_multi(attrib_types_dict, projected_attrib, projection_names, query)
        projection_sol = self.find_solution_on_multi(attrib_types_dict, projected_attrib, projection_names, projection_dep, query)
        self.build_equation(projected_attrib, projection_dep, projection_sol)
        self.projected_attribs = projected_attrib
        self.projection_names = projection_names
        self.dependencies = projection_dep
        self.solution = projection_sol
        print("Result", projection_names, projected_attrib, projection_sol, self.param_list)
        return True

    def find_projection_on_filtered_attribs(self, attrib_types_dict, projected_attrib, query, value_used):
        # some projections still not identified.
        newfilterList = copy.deepcopy(self.global_filter_predicates)
        while '' in projected_attrib and len(newfilterList):
            self.truncate_core_relations()

            curr_attrib, curr_value, value_used = construct_value_used_for_filtered_attribs(
                attrib_types_dict, newfilterList, value_used)

            for entry in self.global_filter_predicates:
                add_value_used_for_one_filtered_attrib(attrib_types_dict, curr_attrib, curr_value, entry,
                                                       value_used)

            value_used = self.construct_values_for_attribs(value_used, attrib_types_dict)
            new_result = self.app.doJob(query)
            if isQ_result_empty(new_result):
                print("Unmasque: \n some error in generating new database. Result is empty. Can not identify "
                      "projections completely.")
                return False
            self.analyze2(curr_attrib, curr_value, new_result, newfilterList, projected_attrib)
        return True

    def find_projection_on_unfiltered_attribs(self, attrib_types_dict, query):
        self.truncate_core_relations()
        value_used = self.construct_values_used(attrib_types_dict)
        #print("Values 1", value_used)
        value_used = self.construct_values_for_attribs(value_used, attrib_types_dict)
        #print("Values 2", value_used)
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            print("Unmasque: \n some error in generating new database. Result is empty. Can not identify "
                  "projections completely.")
            return [], [], value_used, False
        projection_names = list(new_result[0])
        new_result = list(new_result[1])
        new_result = [x.strip() for x in new_result]
        projected_attrib = []
        projected_attrib1 = []
        self.analyze1(new_result, projected_attrib1, projected_attrib, value_used)
        for i in range(0, len(projected_attrib)):
            if projected_attrib[i] == '' and projected_attrib1[i] != '':
                projected_attrib[i] = projected_attrib1[i]
        return projected_attrib, projection_names, value_used, True

    def analyze1(self, new_result, projectedAttrib1, projected_attrib, value_used):
        for val in new_result:
            # check for value being decimal
            if is_number(val):
                new_val = str(int(float(val)))
            else:
                new_val = val
            self.update_projections_as_per_new_val(new_val, projectedAttrib1, value_used)

        for val in new_result:
            val2 = val
            if is_number(val):
                if val2.isdigit():
                    new_val = str(int(float(val)))
                else:
                    new_val = str(float(val))
            else:
                new_val = val
            self.update_projections_as_per_new_val(new_val, projected_attrib, value_used)

    def update_projections_as_per_new_val(self, new_val, projectedAttrib1, value_used):
        if new_val in value_used and not (
                any(value_used[value_used.index(new_val) - 1] in i for i in self.global_filter_predicates)):
            projectedAttrib1.append(value_used[value_used.index(new_val) - 1])

        else:
            projectedAttrib1.append('')

    def analyze2(self, curr_attrib, curr_value, new_result, newfilterList, projected_attrib):
        # Analyze values of this new result
        new_result = list(new_result[1])
        new_result = [x.strip() for x in new_result]
        for i in range(len(new_result)):
            # check for value being decimal
            if is_number(new_result[i]):
                new_result[i] = str(int(float(new_result[i])))
        for i in range(len(new_result)):
            if projected_attrib[i] == '' and str(new_result[i]) == str(curr_value).strip():
                projected_attrib[i] = curr_attrib[0][1]
                if len(curr_attrib) > 1:
                    newfilterList.remove(curr_attrib[0])
                    del (curr_attrib[0])
        for val in curr_attrib:
            newfilterList.remove(val)

    def find_dependencies_on_multi(self, attrib_types_dict, projected_attrib, projection_names, query):
        self.truncate_core_relations()
        projection_dep = []
        indices_to_check = []
        print("Projected Attrib", projected_attrib)
        for i in range(len(projected_attrib)):
            if projected_attrib[i] != '':
                projection_dep.append([("identical_expr_nc", projected_attrib[i])])
            else:
                projection_dep.append([])
                indices_to_check.append(i)
        #print("Indices To check", indices_to_check)
        value_used = self.construct_values_used(attrib_types_dict)
        value_used = self.construct_values_for_attribs(value_used, attrib_types_dict)
        prev_result = self.app.doJob(query)
        for idx in indices_to_check:
            projection_dep[idx] = self.get_dependence(idx, projection_names, query, prev_result, attrib_types_dict, value_used)
        print("Dependencies", projection_dep)
        return projection_dep

    def get_dependence(self, index, names, query, prev_res, attrib_types_dict, value_used):
        dep_list = []
        for tab_idx in range(len(self.core_relations)):
            tabname = self.core_relations[tab_idx]
            attrib_list = self.global_all_attribs[tab_idx]
            coinc = 0
            update_value = None
            attrib_idx = 0
            while attrib_idx < len(attrib_list):
                attrib = attrib_list[attrib_idx]
                #print("searching column", attrib, coinc)
                #print("Dependencies", dep_list)
                fil = 0
                join = 0
                for pred in self.global_filter_predicates:
                    if pred[0] == tabname and pred[1] == attrib:
                        fil = pred
                        break
                for elt in self.global_join_graph:
                    for val in elt:
                        if val == attrib:
                            join = elt
                            break
                    if join != 0:
                        break
                

                if fil!=0:
                    #print("Filter", fil)
                    if coinc ==0:
                        update_value = fil[3]
                    else:
                        if 'date' in attrib_types_dict[(tabname, attrib)]: 
                            update_value = fil[4]- datetime.timedelta(days=1)
                        else: 
                            update_value = fil[4]
                update_multi = []
                if join != 0:
                    attrib_idx += 1
                    continue
                    dummy_val = get_dummy_val_for('int')
                    if fil != 0:
                        dummy_val = update_value
                    for val in join:
                        update_multi.append(val)
                        update_multi.append(dummy_val)
                #print("Join Update", update_multi)
                if fil==0 and join==0:
                    if 'int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]:
                        update_value = get_unused_dummy_val('int', value_used)
                        value_used[value_used.index(attrib)+1] = update_value
                            
                    elif 'date' in attrib_types_dict[(tabname, attrib)]:
                        update_value = get_unused_dummy_val('date', value_used)
                        value_used[value_used.index(attrib)+1] = update_value

                    elif 'boolean' in attrib_types_dict[(tabname, attrib)]:
                        if coinc == 0:
                            update_value = constants.dummy_boolean
                        else: 
                            update_value = not update_value
                    elif 'bit varying' in attrib_types_dict[(tabname, attrib)]: 
                        if coinc == 0:
                            update_value = constants.dummy_varbit
                        else:
                            update_value += format(1, 'b')
                    else:
                        update_value = get_unused_dummy_val('char', value_used)
                        #print("Char", update_value)
                        value_used[value_used.index(attrib)+1] = update_value
                if 'char' in attrib_types_dict[(tabname, attrib)] or 'date' in attrib_types_dict[(tabname, attrib)]:
                    update_value = f"'{update_value}'"

                self.update_attrib_in_table(attrib, update_value, tabname)
                # Current Problem with joins, if the attribute is part of join change the corresponding ones as well.
                #print("Prev", prev_res)
                new_result = self.app.doJob(query)
                #print("New", new_result)
                if prev_res[1][index] != new_result[1][index]:
                    dep_list.append((tabname, attrib))
                    prev_res = new_result
                    attrib_idx+=1
                    coinc = 0
                elif coinc == 0:
                    coinc = 1
                else:
                    coinc = 0
                    attrib_idx +=1
        return dep_list
                
    def find_solution_on_multi(self, attrib_types_dict, projected_attrib, projection_names, projection_dep, query):
        solution = []
        for idx_pro, ele in enumerate(projected_attrib):
            print("ele being checked", ele, idx_pro)
            if projection_dep[idx_pro] == [] or (len(projection_dep[idx_pro]) < 2 and projection_dep[idx_pro][0][0] == "identical_expr_nc"):
                print("Simple Projection, Continue")
                solution.append([])
                self.param_list.append([])
            else:
                self.truncate_core_relations()
                value_used = self.construct_values_used(attrib_types_dict)
                value_used = self.construct_values_for_attribs(value_used, attrib_types_dict)  
                prev_result = self.app.doJob(query)
                solution.append(self.get_solution(attrib_types_dict,projection_dep, projection_names, idx_pro, prev_result, value_used, query))
        return solution


    def get_solution(self, attrib_types_dict, projection_dep, projection_names, idx, prev_res, value_used, query):
        dep = projection_dep[idx]
        n = len(dep)
        fil_check = []
        local_param_list = []
        for ele in dep:
            fil = 0
            for pred in self.global_filter_predicates:
                if pred[0] == ele[0] and pred[1] == ele[1]:
                    fil = pred
                    break
            if fil == 0:
                fil_check.append(False)
            else:
                fil_check.append(fil)
        coeff = np.zeros((2**n, 2**n))
        for i in range(n):
            coeff[0][i] = value_used[value_used.index(dep[i][1]) +1]
            local_param_list.append(value_used[value_used.index(dep[i][1])])
        ele = 1
        for i in range(n, 2**n-1):
            ele = int(i/n)
            coeff[0][i] = coeff[0][(i-n)]*coeff[0][(i+ele)%n]
            local_param_list.append(local_param_list[(i-n)] + "*" + local_param_list[(i+ele)%n])
        coeff[0][2**n-1] = 1
        print("Param List", local_param_list)
        self.param_list.append(local_param_list)
        curr_rank = 1
        outer_idx = 1
        while outer_idx < 2**n and curr_rank < 2**n:
            for j in  range(n):
                mi = 1
                ma = 999
                if fil_check[j]:
                    mi = fil_check[j][3]
                    ma = fil_check[j][4]
                coeff[outer_idx][j] = random.randrange(mi, ma)
            ele = 1
            for j in range(n, 2**n-1):
                ele = int(j/n)
                coeff[outer_idx][j] = coeff[outer_idx][(j-n)]*coeff[outer_idx][(j+ele)%n]
            coeff[outer_idx][2**n-1] = 1
            if np.linalg.matrix_rank(coeff) > curr_rank:
                curr_rank += 1
                outer_idx+=1
            
        b = np.zeros((2**n, 1))
        for i in range(2**n):
            for j in range(n):
                tabname = dep[j][0]
                col = dep[j][1]
                value = coeff[i][j]
                self.update_attrib_in_table(col, value, tabname)
            b[i][0] = self.app.doJob(query)[1][idx]
            
        solution = np.linalg.solve(coeff, b)
        solution = np.around(solution, decimals=0)
        print("Equation", coeff, b)
        print("Solution", solution)
        return solution

    def build_equation(self, projected_attrib, projection_dep, projection_sol):
        for idx_pro, ele in enumerate(projected_attrib):
            if projection_dep[idx_pro] == [] or (len(projection_dep[idx_pro]) < 2 and projection_dep[idx_pro][0][0] == "identical_expr_nc"):
                continue
            else:
                projected_attrib[idx_pro] = self.build_equation_helper(projection_sol[idx_pro], projection_dep[idx_pro], self.param_list[idx_pro])
    
    def build_equation_helper(self, solution, dependencies, param_l):
        res_str = ""
        for i in range(len(solution)):
            if solution[i][0] == 0:
                continue
            if i==0:
                res_str += (str(solution[i][0])+"*"+param_l[i]) if solution[i][0]!=1 else param_l[i]
            elif i == len(solution)-1:
                if solution[i][0] > 0:
                    res_str += "+"
                res_str += str(solution[i][0])
            else:
                if solution[i][0] > 0:
                    res_str += "+"
                res_str += (str(solution[i][0])+"*"+param_l[i]) if solution[i][0]!=1 else param_l[i]
        print("Result String", res_str)
        return res_str