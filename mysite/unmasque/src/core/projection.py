import math
import random

import numpy as np
from sympy import symbols, expand, collect, nsimplify

from ...src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase
from ...src.util.utils import count_empty_lists_in, find_diff_idx
from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.core.dataclass.generation_pipeline_package import PackageForGenPipeline
from ...src.util import constants
from ...src.util.aoa_utils import get_LB, get_UB

def if_dependencies_found_incomplete(projection_names, projection_dep):
    if len(projection_names) > 2:
        empty_deps = count_empty_lists_in(projection_dep)
        if len(projection_dep) - empty_deps < 2:
            return True
    return False


def get_index_of_difference(attrib, new_result, new_result1, projection_dep, tabname):
    new_result1 = new_result1[1:]
    diff = find_diff_idx(new_result1, new_result)
    if diff:
        for d in diff:
            if (tabname, attrib) not in projection_dep[d]:
                projection_dep[d].append((tabname, attrib))
    return projection_dep


class Projection(GenerationPipeLineBase):
    def __init__(self, connectionHelper: AbstractConnectionHelper, genPipelineCtx: PackageForGenPipeline):
        super().__init__(connectionHelper, "Projection", genPipelineCtx)
        self.projection_names = None
        self.projected_attribs = None
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
        projected_attrib, projection_names, projection_dep, check = self.find_projection_dependencies(query, s_values)

        if not check:
            self.logger.error("Line 51 : Some problem while identifying the dependency list!")
            return False
        if if_dependencies_found_incomplete(projection_names, projection_dep):
            for s_v in s_values:
                if s_v[2] is not None:
                    self.update_with_val(s_v[1], s_v[0], s_v[2])
        projected_attrib, projection_names, projection_dep, check = self.find_projection_dependencies(query, s_values)
        if not check:
            self.logger.error("Line 59 : Some problem while identifying the dependency list!")
            return False

        self.logger.debug("Line 62 : Projection Deps: ", projection_dep)
        projection_sol = self.find_solution_on_multi(projected_attrib, projection_dep, query)
        self.projected_attribs = projected_attrib
        self.projection_names = projection_names
        self.dependencies = projection_dep
        self.solution = projection_sol
        self.logger.debug("Line 68 : Result ", projection_names, projected_attrib, projection_sol, self.param_list)
        return True

    def find_projection_dependencies(self, query, s_values):
        new_result = self.app.doJob(query)
        if self.app.isQ_result_empty(new_result):
            self.logger.error("Line 74 : Unmasque: \n some error in generating new database. "
                              "Result is empty. Can not identify "
                              "projections completely.")
            return [], [], [], False

        projection_names = list(new_result[0])
        new_result = new_result[1:]
        projected_attrib = []
        keys_to_skip = []
        projection_dep = []
        for i in range(len(projection_names)):
            projection_dep.append([])

        s_value_dict = {}

        for entry in self.global_attrib_types:
            tabname = entry[0]
            attrib = entry[1]
            self.logger.debug("Line 92 : Joined attribs: ", self.joined_attribs)
            self.logger.debug("Line 93 : checking for ", tabname, attrib)
            if attrib in self.joined_attribs:
                val, keys_to_skip = self.check_impact_of_bulk_attribs(attrib, new_result, projection_dep, query,
                                                                      tabname, keys_to_skip, s_value_dict)
            else:
                val = self.check_impact_of_single_attrib(attrib, new_result, projection_dep, query,
                                                         tabname)
            s_values.append((tabname, attrib, val))

        for i in range(len(projection_names)):
            if len(projection_dep[i]) == 1:
                attrib_tup = projection_dep[i][0]
                projected_attrib.append(attrib_tup[1])
            else:
                projected_attrib.append('')

        return projected_attrib, projection_names, projection_dep, True

    def check_impact_of_bulk_attribs(self, attrib, new_result, projection_dep, query, tabname,
                                     keys_to_skip, s_value_dict):
        if attrib in keys_to_skip:
            return s_value_dict[attrib], keys_to_skip
        join_tabnames = []
        other_attribs = self.get_other_attribs_in_eqJoin_grp(attrib)
        val, prev = self.update_attrib_to_see_impact(attrib, tabname)
        self.update_attribs_bulk(join_tabnames, other_attribs, val)
        self.see_d_min()
        new_result1 = self.app.doJob(query)
        self.update_with_val(attrib, tabname, prev)
        self.update_attribs_bulk(join_tabnames, other_attribs, prev)

        if not self.app.isQ_result_empty(new_result1):
            projection_dep = get_index_of_difference(attrib, new_result, new_result1, projection_dep, tabname)
        keys_to_skip = keys_to_skip + other_attribs
        for other_attrib in other_attribs:
            s_value_dict[other_attrib] = val
        return val, keys_to_skip

    def check_impact_of_single_attrib(self, attrib, new_result, projection_dep, query, tabname):
        for fe in self.global_filter_predicates:
            if fe[1] == attrib and (fe[2] == 'equal' or fe[2] == '=') and fe[1] not in self.joined_attribs:
                return
        val, prev = self.update_attrib_to_see_impact(attrib, tabname)
        if val == prev:
            self.logger.debug("Line 137 : Could not find other s-value! Cannot verify impact!")
            return
        new_result1 = self.app.doJob(query)
        self.update_with_val(attrib, tabname, prev)
        if not self.app.isQ_result_empty(new_result1):
            projection_dep = get_index_of_difference(attrib, new_result, new_result1, projection_dep, tabname)
        else:
            self.logger.debug("Line 144 : Got empty result!!!!")
        return val

    def find_solution_on_multi(self, projected_attrib, projection_dep, query):
        solution = []
        for idx_pro, ele in enumerate(projected_attrib):
            self.logger.debug("Line 150 : ele being checked", ele, idx_pro)
            if projection_dep[idx_pro] == [] or (
                    len(projection_dep[idx_pro]) < 2 and projection_dep[idx_pro][0][0] == constants.IDENTICAL_EXPR):
                self.logger.debug("Line 153 : Simple Projection, Continue")
                # Identical output column, so append empty list and continue
                solution.append([])
                self.param_list.append([])
                self.syms.append([])
            else:
                value_used = self.construct_value_used_with_dmin()
                self.logger.debug("Line 160 : Inside else", value_used)
                solution.append(
                    self.get_solution(projected_attrib, projection_dep, idx_pro, value_used, query))
        return solution

    """
    Solve Ax=b to get the expression of the output column
    """

    def get_solution(self, projected_attrib, projection_dep, idx, value_used, query):
        self.logger.debug("Line 170 : filters: ", self.global_filter_predicates)
        dep = projection_dep[idx]
        n = len(dep)
        fil_check = []
        sym_string = ''
        for i in dep:
            sym_string += (i[1] + " ")
        res = 1
        if n > 1:
            syms = symbols(sym_string)
            local_symbol_list = syms
            syms = sorted(syms, key=lambda x: str(x))
            self.logger.debug("Line 182 : symbols", syms)
            for i in syms:
                res *= (1 + i)
            self.logger.debug("Line 185 : Sym List", expand(res).args)
            self.syms.append(get_param_values_external(syms))
            self.logger.debug("Line 187 : Another List", self.syms)
        else:
            self.syms.append([symbols(sym_string)])
            local_symbol_list = self.syms[-1]
            self.logger.debug("Line 191 : Another List", self.syms, idx)
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
        self.logger.debug("Line 221 : Param List", local_param_list)
        self.param_list.append(local_param_list)
        self.infinite_loop(coeff, fil_check, n)
        self.logger.debug("Line 224 : Coeff", coeff)
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
                        for tab_key in self.global_all_attribs.keys():
                            if val in self.global_all_attribs[tab_key]:
                                update_multi.append(tab_key)
                                break
                        update_multi.append(value)
                    for inner_i in range(0, len(update_multi), 3):
                        self.update_attrib_in_table(update_multi[inner_i], update_multi[inner_i + 2],
                                                    update_multi[inner_i + 1])

            # print(self.app.doJob(query), b)
            exe_result = self.app.doJob(query)
            if not self.app.isQ_result_empty(exe_result):
                b[i][0] = self.app.get_attrib_val(exe_result, idx)
        self.logger.debug("Line 261 : Coeff", coeff)
        solution = np.linalg.solve(coeff, b)
        solution = np.around(solution, decimals = 0)
        final_res = 0
        for i, ele in enumerate(self.syms[idx]):
            final_res += (ele * solution[i])
        self.logger.debug("Line 267 : Coeff of 1", solution[len(self.syms[idx]) - 1])
        final_res += 1 * solution[-1]
        self.logger.debug("Line 269 : Equation", coeff, b)
        self.logger.debug("Line 270 : Solution", solution)
        # self.logger.debug("Final", final_res, nsimplify(collect(final_res, local_symbol_list)))
        projected_attrib[idx] = str(nsimplify(collect(final_res, local_symbol_list)))
        return solution

    def infinite_loop(self, coeff, fil_check, n):
        curr_rank = 1
        outer_idx = 1
        while outer_idx < 2 ** n and curr_rank < 2 ** n:
            prev_idx, prev_rank = outer_idx, curr_rank
            # Same algorithm as above with insertion of random values
            # Additionally checking if rank of the matrix has become 2^n
            for j in range(n):
                pred = fil_check[j]
                min = constants.pr_min
                max = constants.pr_max
                if pred:
                    datatype = self.get_datatype((fil_check[j][0], fil_check[j][1]))
                    min = get_LB(pred)
                    max = get_UB(pred)
                if datatype == 'int':
                    coeff[outer_idx][j] = random.randrange(min, max)
                elif (datatype == 'numeric'):
                    coeff[outer_idx][j] = random.uniform(min, max)
            temp_array = get_param_values_external(coeff[outer_idx][:n])
            for j in range(2 ** n - 1):
                coeff[outer_idx][j] = temp_array[j]
            coeff[outer_idx][2 ** n - 1] = 1.0
            m_rank = np.linalg.matrix_rank(coeff)
            if m_rank > curr_rank:
                curr_rank += 1
                outer_idx += 1
            self.logger.debug("Line 296 : outer_idx: ", outer_idx, "curr_rank: ", curr_rank)
            if prev_idx == outer_idx and curr_rank == prev_rank:
                self.logger.debug("Line 298 : It will go to infinite loop!! so breaking...")
                break

    def build_equation(self, projected_attrib, projection_dep, projection_sol):
        # print("Full list", self.param_list)
        for idx_pro, ele in enumerate(projected_attrib):
            if projection_dep[idx_pro] == [] or projection_sol[idx_pro] == []:
                continue
            else:
                projected_attrib[idx_pro] = self.build_equation_helper(projection_sol[idx_pro],
                                                                       self.param_list[idx_pro])

    def build_equation_helper(self, solution, dependencies, param_l):
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

        self.logger.debug("Line 326 : Result String", res_str)
        return res_str

    def get_param_list(self, deps):
        self.logger.debug("Line 330 : ",deps)
        subsets = get_subsets(deps)
        subsets = sorted(subsets, key=len)
        self.logger.debug("Line 333 : ",subsets)
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
        used_val = [val for data in self.global_min_instance_dict.values() for pair in zip(*data) for val in pair]
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
