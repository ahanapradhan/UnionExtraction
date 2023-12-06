import math
import random

import numpy as np
from sympy import symbols, expand, collect, nsimplify

from ..refactored.util.utils import get_unused_dummy_val, get_val_plus_delta, count_empty_lists_in
from ..src.core.abstract.ProjectionBase import ProjectionBase
from ..src.util import constants


def if_dependencies_found_incomplete(projection_names, projection_dep):
    if len(projection_names) > 2:
        empty_deps = count_empty_lists_in(projection_dep)
        if len(projection_dep) - empty_deps < 2:
            return True
    return False


class Projection(ProjectionBase):
    def __init__(self, connectionHelper, global_attrib_types, core_relations, filter_predicates, join_graph,
                 global_all_attribs, global_min_instance_dict, global_key_attribs):
        super().__init__(connectionHelper, "projection", global_all_attribs, global_attrib_types, global_key_attribs,
                         core_relations, join_graph, filter_predicates, global_min_instance_dict, global_attrib_types)
        self.dependencies = None
        self.solution = None
        self.syms = []
        """
        List of list of all the subsets of the dependencies of an output column (having more than one dependencies)
        Suppose a column is dependent on a and b, corresponding index of that column in param_list will contain, [a,b,a*b]
        """
        self.param_list = []

    def doExtractJob(self, query):
        s_values = []
        projected_attrib, projection_names, projection_dep, check = self.find_dep_one_round(query, s_values)
        if if_dependencies_found_incomplete(projection_names, projection_dep):
            for s_v in s_values:
                if s_v[2] is not None:
                    self.update_with_val((s_v[1], s_v[0]), s_v[2])
        projected_attrib, projection_names, projection_dep, check = self.find_dep_one_round(query, s_values)
        if not check:
            return False

        # projection_dep = self.find_dependencies_on_multi(self.attrib_types_dict, projected_attrib,
        # projection_names,query)
        projection_sol = self.find_solution_on_multi(projected_attrib, projection_names,
                                                     projection_dep, query)
        # self.build_equation(projected_attrib, projection_dep, projection_sol)
        self.dependencies = projection_dep
        self.solution = projection_sol
        self.projected_attribs = projected_attrib
        self.projection_names = projection_names
        self.logger.debug("Result ", projection_names, projected_attrib, projection_sol, self.param_list)
        return check

    def find_dependencies_on_multi(self, projected_attrib, query):
        projection_dep = []
        indices_to_check = []
        self.logger.debug("Projected Attrib", projected_attrib)
        for i in range(len(projected_attrib)):
            # Construct the initial dependency list
            projection_dep.append([])
            indices_to_check.append(i)
        self.logger.debug("Indices To check", indices_to_check)
        value_used = self.construct_value_used_with_dmin()
        # Prev Result to check for changes
        prev_result = self.app.doJob(query)
        for idx in indices_to_check:
            projection_dep[idx], prev_result = self.get_dependence(idx, query, prev_result, value_used)
        self.logger.debug("Dependencies", projection_dep)
        return projection_dep

    def get_dependence(self, index, query, prev_res, value_used):
        dep_list = []
        self.logger.debug("IDX", index)
        to_be_skipped = []
        for tab_idx in range(len(self.core_relations)):
            tabname = self.core_relations[tab_idx]
            attrib_list = self.global_all_attribs[tab_idx]
            coinc = 0  # Coincidence
            update_value = None
            attrib_idx = 0
            while attrib_idx < len(attrib_list):
                attrib = attrib_list[attrib_idx]
                self.logger.debug("Attrib", attrib)
                if attrib in to_be_skipped:
                    attrib_idx += 1
                    continue
                # print(attrib)
                fil = 0
                join = 0

                # Check if the attribute is part of Filter or Join Predicates
                for pred in self.global_filter_predicates:
                    if pred[0] == tabname and pred[1] == attrib:
                        fil = pred
                        break
                for elt in self.global_join_graph:
                    for val in elt:
                        if val == attrib:
                            join = elt
                            break
                    if join:
                        break

                if fil:
                    # Handle attributes involved in filter predicates.
                    if not coinc:
                        update_value = fil[3]  # Min Value for first test
                    else:
                        # Take Max value for to check for coincidence
                        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
                            update_value = get_val_plus_delta('date', fil[4], -1)
                        elif fil[2] == 'LIKE':
                            update_value = fil[4].replace('%', 'a')
                        else:
                            update_value = fil[4]
                update_multi = []
                if join:
                    # Code to be added later for attribs involved in join
                    dummy_val = get_unused_dummy_val('int', value_used)
                    value_used.append(attrib)
                    value_used.append(dummy_val)
                    if fil:
                        dummy_val = update_value
                    for val in join:
                        to_be_skipped.append(val)
                        update_multi.append(val)
                        for idx, ele in enumerate(self.global_all_attribs):
                            if val in ele:
                                update_multi.append(self.core_relations[idx])
                        update_multi.append(dummy_val)
                self.logger.debug("Join Update", update_multi)
                # prev_val = value_used[value_used.index(attrib) + 1]
                # self.logger.debug("Val to restore", prev)
                if not fil and not join:
                    if 'int' in self.attrib_types_dict[(tabname, attrib)] \
                            or 'numeric' in self.attrib_types_dict[(tabname, attrib)]:
                        update_value = get_unused_dummy_val('int', value_used)
                        value_used[value_used.index(attrib) + 1] = update_value

                    elif 'date' in self.attrib_types_dict[(tabname, attrib)]:
                        update_value = get_unused_dummy_val('date', value_used)
                        value_used[value_used.index(attrib) + 1] = update_value

                    elif 'boolean' in self.attrib_types_dict[(tabname, attrib)]:
                        if not coinc:
                            update_value = constants.dummy_boolean
                        else:
                            update_value = not update_value
                    elif 'bit varying' in self.attrib_types_dict[(tabname, attrib)]:
                        if not coinc:
                            update_value = constants.dummy_varbit
                        else:
                            update_value += format(1, 'b')
                    else:
                        update_value = get_unused_dummy_val('char', value_used)
                        self.logger.debug("Char", update_value)
                        value_used[value_used.index(attrib) + 1] = update_value
                if 'char' in self.attrib_types_dict[(tabname, attrib)] or 'date' in self.attrib_types_dict[
                    (tabname, attrib)]:
                    self.logger.debug("if pred")
                    update_value = f"'{update_value}'"
                # print("updated", attrib, update_value)
                if not join:
                    self.logger.debug("Updated values", attrib, update_value)
                    self.update_attrib_in_table(attrib, update_value, tabname)
                else:
                    self.logger.debug("Updated values", attrib, update_multi)
                    for i in range(0, len(update_multi), 3):
                        self.update_attrib_in_table(update_multi[i], update_multi[i + 2], update_multi[i + 1])
                # Current Problem with joins, if the attribute is part of join change the corresponding ones as well.

                self.logger.debug("Prev", prev_res)
                new_result = self.app.doJob(query)
                self.logger.debug("New", new_result)
                if len(new_result) > 1 and prev_res[1][index] != new_result[1][index]:
                    dep_list.append((tabname, attrib))
                    # prev_res = new_result
                    attrib_idx += 1
                    coinc = 0
                elif coinc == 0:
                    # Try again to check for coincidence
                    coinc = 1
                else:
                    # Not a coincidence
                    coinc = 0
                    attrib_idx += 1
                prev_res = new_result
        return dep_list, prev_res

    def find_solution_on_multi(self, projected_attrib, projection_names, projection_dep, query):
        solution = []
        for idx_pro, ele in enumerate(projected_attrib):
            self.logger.debug("ele being checked", ele, idx_pro)
            if projection_dep[idx_pro] == [] or (
                    len(projection_dep[idx_pro]) < 2 and projection_dep[idx_pro][0][0] == constants.IDENTICAL_EXPR):
                self.logger.debug("Simple Projection, Continue")
                # Identical output column, so append empty list and continue
                solution.append([])
                self.param_list.append([])
                self.syms.append([])
            else:
                value_used = self.construct_value_used_with_dmin()
                prev_result = self.app.doJob(query)
                self.logger.debug("Inside else", value_used)
                solution.append(
                    self.get_solution(projected_attrib, projection_dep, projection_names, idx_pro, prev_result,
                                      value_used, query))
        return solution

    """
    Solve Ax=b to get the expression of the output column
    """

    def get_solution(self, projected_attrib, projection_dep, projection_names, idx, prev_res, value_used, query):
        dep = projection_dep[idx]
        n = len(dep)
        fil_check = []
        local_param_list = []  # param_list for only this output column
        local_symbol_list = []
        sym_string = ''
        for i in dep:
            sym_string += (i[1] + " ")
        res = 1
        if n > 1:
            syms = symbols(sym_string)
            local_symbol_list = syms
            syms = sorted(syms, key=lambda x: str(x))
            self.logger.debug("symbols", syms)
            for i in syms:
                res *= (1 + i)
            self.logger.debug("Sym List", expand(res).args)
            self.syms.append(get_param_values_external(syms))
            self.logger.debug("Another List", self.syms)
        else:
            self.syms.append([symbols(sym_string)])
            local_symbol_list = self.syms[-1]
            self.logger.debug("Another List", self.syms, idx)
        if n == 1 and ('int' not in self.attrib_types_dict[(dep[0][0], dep[0][1])]) and (
                'numeric' not in self.attrib_types_dict[(dep[0][0], dep[0][1])]):
            self.param_list.append([dep[0][1]])
            projected_attrib[idx] = dep[0][1]
            return [[1]]
        for ele in dep:
            # Construct a list of list that will be used to check if the attrib belongs to filter predicate
            fil = 0
            for pred in self.global_filter_predicates:
                if pred[0] == ele[0] and pred[1] == ele[1]:
                    fil = pred
                    break
            if not fil:
                fil_check.append(False)
            else:
                fil_check.append(fil)

        coeff = np.zeros((2 ** n, 2 ** n))

        for i in range(n):
            coeff[0][i] = value_used[value_used.index(dep[i][1]) + 1]
        temp_array = get_param_values_external(coeff[0][:n])
        for i in range(2 ** n - 1):
            # Given the values of the n dependencies, we form the rest 2^n - n combinations
            coeff[0][i] = temp_array[i]

        coeff[0][2 ** n - 1] = 1

        local_param_list = self.get_param_list(sorted([i[1] for i in dep]))
        self.logger.debug("Param List", local_param_list)
        self.param_list.append(local_param_list)
        curr_rank = 1
        outer_idx = 1
        while outer_idx < 2 ** n and curr_rank < 2 ** n:
            # Same algorithm as above with insertion of random values
            # Additionally checking if rank of the matrix has become 2^n
            for j in range(n):
                mi = constants.pr_min
                ma = constants.pr_max
                if fil_check[j]:
                    mi = fil_check[j][3]
                    ma = fil_check[j][4]
                coeff[outer_idx][j] = random.randrange(math.ceil(mi), math.floor(ma))
            temp_array = get_param_values_external(coeff[outer_idx][:n])
            for j in range(2 ** n - 1):
                coeff[outer_idx][j] = temp_array[j]
            coeff[outer_idx][2 ** n - 1] = 1.0
            if np.linalg.matrix_rank(coeff) > curr_rank:
                curr_rank += 1
                outer_idx += 1
        # print("N", n)
        b = np.zeros((2 ** n, 1))
        for i in range(2 ** n):
            for j in range(n):
                col = self.param_list[idx][j]
                tabname = None
                for e_dep in dep:
                    if col in e_dep:
                        tabname = e_dep[0]
                value = coeff[i][j]
                join = []
                for elt in self.global_join_graph:
                    if dep[j][1] in elt:
                        join = elt
                        break
                    if join:
                        break
                if not join:
                    self.update_attrib_in_table(col, value, tabname)
                else:
                    update_multi = []
                    for val in join:
                        update_multi.append(val)
                        for l, ele in enumerate(self.global_all_attribs):
                            if val in ele:
                                update_multi.append(self.core_relations[l])
                                break
                        update_multi.append(value)
                    for inner_i in range(0, len(update_multi), 3):
                        self.update_attrib_in_table(update_multi[inner_i], update_multi[inner_i + 2],
                                                    update_multi[inner_i + 1])

            # print(self.app.doJob(query), b)
            exe_result = self.app.doJob(query)
            if len(exe_result) > 1:
                b[i][0] = exe_result[1][idx]

        solution = np.linalg.solve(coeff, b)
        solution = np.around(solution, decimals=0)
        final_res = 0
        for i, ele in enumerate(self.syms[idx]):
            final_res += (ele * solution[i])
        self.logger.debug("Coeff of 1", solution[len(self.syms[idx]) - 1])
        final_res += 1 * solution[-1]
        self.logger.debug("Equation", coeff, b)
        self.logger.debug("Solution", solution)
        # self.logger.debug("Final", final_res, nsimplify(collect(final_res, local_symbol_list)))
        projected_attrib[idx] = str(nsimplify(collect(final_res, local_symbol_list)))
        return solution

    def build_equation(self, projected_attrib, projection_dep, projection_sol):
        # print("Full list", self.param_list)
        for idx_pro, ele in enumerate(projected_attrib):
            if projection_dep[idx_pro] == [] or projection_sol[idx_pro] == []:
                continue
            else:
                projected_attrib[idx_pro] = self.build_equation_helper(projection_sol[idx_pro],
                                                                       self.param_list[idx_pro])

    def build_equation_helper(self, solution, dependencies, param_l):
        n = len(dependencies)
        # syms = " ".join(dependencies[:n])
        # syms = symbols(syms)
        # expr = None
        res_str = ""
        for i in range(len(solution)):
            if solution[i][0] == 0:
                continue
            if res_str == "":
                res_str += (str(solution[i][0]) + "*" + param_l[i]) if solution[i][0] != 1 else param_l[i]
            elif i == len(solution) - 1:
                if solution[i][0] > 0:
                    res_str += "+"
                res_str += str(solution[i][0])
            else:
                if solution[i][0] > 0:
                    res_str += "+"
                res_str += (str(solution[i][0]) + "*" + param_l[i]) if solution[i][0] != 1 else param_l[i]

        self.logger.debug("Result String", res_str)
        return res_str

    def get_param_list(self, deps):
        self.logger.debug(deps)
        subsets = get_subsets(deps)
        subsets = sorted(subsets, key=len)
        self.logger.debug(subsets)
        final_lis = []
        for i in subsets:
            if len(i) < 2 and i == []:
                continue
            temp_str = ""
            for j in range(len(i)):
                temp_str += i[j]
                if j != len(i) - 1:
                    temp_str += "*"
            final_lis.append(temp_str)
        return final_lis

    def construct_value_used_with_dmin(self):
        used_val = []
        for tab_name in self.global_min_instance_dict:
            vals = self.global_min_instance_dict[tab_name]
            for idx in range(len(vals[0])):
                used_val.append(vals[0][idx])
                used_val.append(vals[1][idx])
        return used_val


def get_param_values_external(coeff_arr):
    # print(coeff_arr)
    subsets = get_subsets(coeff_arr)
    subsets = sorted(subsets, key=len)
    # print(subsets)
    final_lis = []
    for i in subsets:
        temp_val = 1
        if len(i) < 2 and i == []:
            continue
        for j in range(len(i)):
            temp_val *= i[j]
        final_lis.append(temp_val)
    return final_lis


def get_subsets(deps):
    res = []
    get_subsets_helper(deps, res, [], 0)
    return res


def get_subsets_helper(deps, res, curr, idx):
    res.append(curr[:])
    for i in range(idx, len(deps)):
        curr.append(deps[i])
        get_subsets_helper(deps, res, curr, i + 1)
        curr.pop()
