import math
import random

import numpy as np
from sympy import symbols, expand, collect, nsimplify, Rational, Integer

from .dataclass.genPipeline_context import GenPipelineContext
from ..util.constants import CONST_1_VALUE, NUMBER_TYPES
from ...src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase, get_boundary_value
from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util import constants
from ...src.util.utils import count_empty_lists_in, find_diff_idx, get_format


def round_expr(expr, num_digits):
    return expr.xreplace({n: round(n, num_digits) for n in expr.atoms(Rational) if not isinstance(n, Integer)})


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
    def __init__(self, connectionHelper: AbstractConnectionHelper, genPipelineCtx: GenPipelineContext):
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
        self.rmin_card = 0

    def doExtractJob(self, query):
        s_values = []
        projected_attrib, projection_names, projection_dep, check = self.find_projection_dependencies(query, s_values)

        if not check:
            self.logger.error("Some problem while identifying the dependency list!")
            return False
        if if_dependencies_found_incomplete(projection_names, projection_dep):
            for s_v in s_values:
                if s_v[2] is not None:
                    self.update_with_val(s_v[1], s_v[0], s_v[2])
        projected_attrib, projection_names, projection_dep, check = self.find_projection_dependencies(query, s_values)
        if not check:
            self.logger.error("Some problem while identifying the dependency list!")
            return False

        self.logger.debug("Projection Deps: ", projection_dep)
        projection_sol = self.find_solution_on_multi(projected_attrib, projection_dep, query)
        self.projected_attribs = projected_attrib
        self.projection_names = projection_names
        self.dependencies = projection_dep
        self.solution = projection_sol
        self.logger.debug("Result ", projection_names, projected_attrib, projection_sol, self.param_list)
        return True

    def find_projection_dependencies(self, query, s_values):
        new_result = self.app.doJob(query)
        if self.app.isQ_result_empty(new_result):
            self.logger.error("Unmasque: \n some error in generating new database. "
                              "Result is empty. Can not identify "
                              "projections completely.")
            return [], [], [], False

        self.rmin_card = len(new_result)

        projection_names = list(new_result[0])
        projection_coulumn_type = self.get_projection_column_type(query, projection_names)

        new_result = new_result[1:]
        projected_attrib = []
        keys_to_skip = []
        projection_dep = [[] for _ in projection_names]
        s_value_dict = {}

        for entry in self.global_attrib_types:
            tabname = entry[0]
            attrib = entry[1]
            self.logger.debug("Joined attribs: ", self.joined_attribs)
            self.logger.debug("checking for ", tabname, attrib)
            if attrib in self.joined_attribs:
                val, keys_to_skip = self.check_impact_of_bulk_attribs(attrib, new_result, projection_dep, query,
                                                                      tabname, keys_to_skip, s_value_dict)
            else:
                val = self.check_impact_of_single_attrib(attrib, new_result, projection_dep, query, tabname)
            s_values.append((tabname, attrib, val))

        for i in range(len(projection_names)):
            if len(projection_dep[i]) == 1:
                attrib_tup = projection_dep[i][0]
                projected_attrib.append(attrib_tup[1])
            else:
                if len(projection_dep[i]) == 0 and new_result[0][i] != CONST_1_VALUE:
                    # If no dependency is there and value is not 1 in result this means it is constant.
                    projected_attrib.append(get_format(projection_coulumn_type[i], new_result[0][i]))
                else:
                    # No dependency and value is one so in groupby file will differentiate between count or 1.
                    projected_attrib.append('')

        return projected_attrib, projection_names, projection_dep, True

    def get_projection_column_type(self, query, projection_names):
        mod_query = query.replace(";", '')
        datatype_names = []
        for i in projection_names:
            typecheck_query = str(
                'SELECT pg_typeof(' + i + ') AS data_type FROM (' + mod_query + ') AS subquery limit 1;')
            type_result = self.app.doJob(typecheck_query)
            modified_str = str(type_result[1])
            modified_str = modified_str[1:-2]
            datatype_names.append(modified_str)
        datatype_names = [element.strip("'") for element in datatype_names]
        return datatype_names

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
            self.logger.debug("Could not find other s-value! Cannot verify impact!")
            return
        new_result1 = self.app.doJob(query)
        self.update_with_val(attrib, tabname, prev)
        if not self.app.isQ_result_empty(new_result1):
            projection_dep = get_index_of_difference(attrib, new_result, new_result1, projection_dep, tabname)
        else:
            self.logger.debug("Got empty result!!!!")
        return val

    def find_solution_on_multi(self, projected_attrib, projection_dep, query):
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
                self.logger.debug("Inside else", value_used)
                solution.append(
                    self.get_solution(projected_attrib, projection_dep, idx_pro, value_used, query))
        return solution

    """
    Solve Ax=b to get the expression of the output column
    """

    def get_solution(self, projected_attrib, projection_dep, idx, value_used, query):
        self.logger.debug("filters: ", self.filter_attrib_dict)
        dep = projection_dep[idx]
        n = len(dep)
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
        if n == 1 and self.attrib_types_dict[(dep[0][0], dep[0][1])] not in NUMBER_TYPES:
            self.param_list.append([dep[0][1]])
            projected_attrib[idx] = dep[0][1]
            return [[1]]

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

        self.__infinite_loop(coeff, dep)
        # print("N", n)
        b = np.zeros((2 ** n, 1))
        for i in range(2 ** n):
            for j in range(n):
                col = self.param_list[idx][j]
                value = coeff[i][j]
                joined_cols = self.get_other_attribs_in_eqJoin_grp(col)
                joined_cols.append(col)
                for j_c in joined_cols:
                    self.update_attrib_in_table(j_c, value, self.find_tabname_for_given_attrib(j_c))

            exe_result = self.app.doJob(query)
            if not self.app.isQ_result_no_full_nullfree_row(exe_result):
                null_free_row = self.app.get_nullfree_row(exe_result)
                b[i][0] = null_free_row[idx]
        self.logger.debug("Coeff", coeff)
        solution = np.linalg.solve(coeff, b)
        solution = np.around(solution, decimals=2)
        final_res = 0
        for i, ele in enumerate(self.syms[idx]):
            final_res += (ele * solution[i])
        self.logger.debug("Coeff of 1", solution[len(self.syms[idx]) - 1])
        final_res += 1 * solution[-1]
        self.logger.debug("Equation", coeff, b)
        self.logger.debug("Solution", solution)
        res_expr = nsimplify(collect(final_res, local_symbol_list))
        projected_attrib[idx] = str(round_expr(res_expr, 2))
        return solution

    def __infinite_loop(self, coeff, dep):
        n = len(dep)
        curr_rank = 1
        outer_idx = 1
        while outer_idx < 2 ** n and curr_rank < 2 ** n:
            prev_idx, prev_rank = outer_idx, curr_rank
            # Same algorithm as above with insertion of random values
            # Additionally checking if rank of the matrix has become 2^n
            for j, ele in enumerate(dep):
                key = (ele[0], ele[1])
                mini = constants.pr_min
                maxi = constants.pr_max
                if key in self.filter_attrib_dict.keys():
                    datatype = self.get_datatype(key)
                    mini = get_boundary_value(self.filter_attrib_dict[key][0], is_ub=False)
                    maxi = get_boundary_value(self.filter_attrib_dict[key][1], is_ub=True)
                    if datatype == 'int':
                        coeff[outer_idx][j] = random.randrange(mini, maxi)
                    elif datatype == 'numeric':
                        coeff[outer_idx][j] = round(random.uniform(mini, maxi), 2)
                else:
                    coeff[outer_idx][j] = random.randrange(math.floor(mini), math.ceil(maxi))

            temp_array = get_param_values_external(coeff[outer_idx][:n])
            for j in range(2 ** n - 1):
                coeff[outer_idx][j] = temp_array[j]
            coeff[outer_idx][2 ** n - 1] = 1.0
            m_rank = np.linalg.matrix_rank(coeff)
            if m_rank > curr_rank:
                curr_rank += 1
                outer_idx += 1
            self.logger.debug("outer_idx: ", outer_idx, "curr_rank: ", curr_rank)
            if prev_idx == outer_idx and curr_rank == prev_rank:
                self.logger.debug("It will go to infinite loop!! so breaking...")
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
