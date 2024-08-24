import ast
import copy
import itertools

import frozenlist as frozenlist

from .dataclass.genPipeline_context import GenPipelineContext
from .dataclass.pgao_context import PGAOcontext
from ...src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase, get_boundary_value
from ..util.constants import NON_TEXT_TYPES
from ...src.util.utils import get_dummy_val_for, get_val_plus_delta, get_format, get_char


class Limit(GenerationPipeLineBase):
    def __init__(self, connectionHelper, genPipelineCtx: GenPipelineContext,
                 genCtx: PGAOcontext):
        super().__init__(connectionHelper, "Limit", genPipelineCtx)
        self.limit = None
        self.global_groupby_attributes = genCtx.group_by_attrib
        self.joined_attrib_valDict = {}
        self.no_rows = self.connectionHelper.config.limit_limit
        self.rmin_card = genCtx.projection

    def doExtractJob(self, query):
        result = self.doLimitExtractJob(query)
        self.do_init()
        return result

    def doLimitExtractJob(self, query):
        grouping_attribute_values = {}
        pre_assignment = self.__get_pre_assignment()
        gb_tab_attribs = [(self.find_tabname_for_given_attrib(attrib), attrib)
                          for attrib in self.global_groupby_attributes]

        total_combinations = 1
        self.__decide_number_of_rows(gb_tab_attribs, grouping_attribute_values, pre_assignment, total_combinations)

        gen_dict = {}

        for table in self.core_relations:
            attrib_list = self.global_all_attribs[table]
            attrib_list_str = ",".join(attrib_list)
            att_order = f"({attrib_list_str})"
            insert_rows = []
            for k in range(self.no_rows):
                self.__determine_k_insert_rows(attrib_list, gb_tab_attribs, grouping_attribute_values, insert_rows,
                                               k, table)
            self.insert_attrib_vals_into_table(att_order, attrib_list, insert_rows, table, insert_logger=False)
            gen_dict[table] = insert_rows

            new_result = self.app.doJob(query)
            if not self.app.isQ_result_empty(new_result):
                """                 
                ideally dmin at the start of proj module should produce 2 row result. header + 1 row data
                but, union + outer join may cause extra invalid data.
                all of them cannot be discarded simply due to having NULL values
                So, assuming ideal data at proj module start, calculating the valid rows in other modules
                """
                fresh_result_card = len(new_result) - self.rmin_card + 2
                if 4 <= fresh_result_card <= self.no_rows:
                    if self.limit is not None and self.limit == fresh_result_card - 1:
                        self.limit = fresh_result_card - 1  # excluding the header column
                        self.logger.debug(f"Finalized Limit {self.limit}")
                        break
                    else:
                        self.limit = fresh_result_card - 1  # excluding the header column
                        self.logger.debug(f"Limit {self.limit}")
                else:
                    if self.limit is not None:
                        self.limit = None
                        self.logger.debug(f"Result is growing. Limit may be larger than {self.no_rows}")
                        break
        return True

    def __determine_k_insert_rows(self, attrib_list_inner, gb_tab_attribs, grouping_attribute_values, insert_rows, k,
                                  tabname_inner):
        insert_values = []
        for attrib_inner in attrib_list_inner:
            datatype = self.get_datatype((tabname_inner, attrib_inner))
            if attrib_inner in grouping_attribute_values.keys():
                insert_values.append(grouping_attribute_values[attrib_inner][k])
            elif attrib_inner not in self.joined_attribs \
                    and (tabname_inner, attrib_inner) not in gb_tab_attribs:
                insert_values.append(self.get_dmin_val(attrib_inner, tabname_inner))
            elif datatype in NON_TEXT_TYPES:
                self.insert_non_text_attrib(datatype, attrib_inner, insert_values, k, tabname_inner)
            else:
                self.insert_text_attrib(attrib_inner, insert_values, k, tabname_inner)
        insert_rows.append(tuple(insert_values))

    def __decide_number_of_rows(self, gb_tab_attribs, grouping_attribute_values, pre_assignment, total_combinations):
        if pre_assignment:
            # GET LIMITS FOR ALL GROUPBY ATTRIBUTES
            group_lists = []
            for elt in gb_tab_attribs:
                temp = []
                if elt not in self.filter_attrib_dict.keys():
                    pre_assignment = False
                    break
                datatype = self.get_datatype(elt)
                if datatype in NON_TEXT_TYPES:
                    tot_values = self.__compute_total_values(datatype, elt, total_combinations)
                    self.__get_temp_total_values(datatype, elt, temp, tot_values)
                else:
                    if '%' in self.filter_attrib_dict[elt] or '_' in self.filter_attrib_dict[elt]:
                        pre_assignment = False
                        break
                    else:
                        temp = [self.filter_attrib_dict[elt]]
                total_combinations = total_combinations * len(temp)
                group_lists.append(copy.deepcopy(temp))

            # CREATE DIFFERENT PERMUTATIONS OF GROUPBY COLUMNS VALUE ASSIGNMENTS HERE
            if pre_assignment:
                combo_values = list(itertools.product(*group_lists))
                for elt in self.global_groupby_attributes:
                    grouping_attribute_values[elt] = []
                for elt in combo_values:
                    temp = list(elt)
                    for (val1, val2) in zip(self.global_groupby_attributes, temp):
                        grouping_attribute_values[val1].append(val2)
        if pre_assignment:
            self.no_rows = min(self.no_rows, total_combinations)

    def __get_temp_total_values(self, datatype, elt, temp, tot_values):
        if datatype == 'date':
            for k in range(tot_values):
                date_val = get_val_plus_delta('date', self.filter_attrib_dict[elt][0], k)
                temp.append(ast.literal_eval(get_format('date', date_val)))
        else:
            lb = get_boundary_value(self.filter_attrib_dict[elt][0], is_ub=False)
            for k in range(tot_values):
                temp.append(lb + k)

    def __compute_total_values(self, datatype, elt, total_combinations):
        tot_values = 0
        for in_pred in self.filter_in_predicates:
            if (in_pred[0], in_pred[1]) == elt:
                value_range = in_pred[3]
                for v in value_range:
                    if not isinstance(v, tuple):
                        tot_values += 1
                    else:
                        tot_values += v[-1] - v[0]
        if not tot_values:
            if datatype == 'date':
                tot_values = (self.filter_attrib_dict[elt][1] - self.filter_attrib_dict[elt][0]).days + 1
            else:
                tot_values = self.filter_attrib_dict[elt][1] - self.filter_attrib_dict[elt][0] + 1
        if (total_combinations * tot_values) > self.no_rows:
            i = 1
            while (total_combinations * i) < self.no_rows + 1 and i < tot_values:
                i = i + 1
            tot_values = i
        return tot_values

    def insert_text_attrib(self, attrib_inner, insert_values, k, tabname_inner):
        char_val = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), k))  # why need unique values?
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            s_val_text = self.get_s_val_for_textType(attrib_inner, tabname_inner)
            temp = copy.deepcopy(s_val_text)
            if '_' in s_val_text:
                insert_values.append(temp.replace('_', char_val))
            else:
                insert_values.append(temp.replace('%', char_val, 1))
            insert_values[-1].replace('%', '')
        else:
            insert_values.append(char_val)

    def insert_non_text_attrib(self, datatype, attrib_inner, insert_values, k, tabname_inner):
        for edge in self.global_join_graph:
            if attrib_inner in edge:
                edge_key = frozenlist.FrozenList(edge)
                edge_key.freeze()
                if edge_key in self.joined_attrib_valDict.keys() \
                        and k < len(self.joined_attrib_valDict[edge_key]):
                    s_val_plus_k = self.joined_attrib_valDict[edge_key][k]
                    insert_values.append(ast.literal_eval(get_format(datatype, s_val_plus_k)))
                    return

        if datatype != 'date':
            datatype = 'int'
        # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
        s_val = self.get_s_val(attrib_inner, tabname_inner)
        s_val_plus_k = get_val_plus_delta(datatype, s_val, k)
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            one = self.filter_attrib_dict[(tabname_inner, attrib_inner)][1]
            one = get_boundary_value(one, is_ub=True)
            s_val_plus_k = min(s_val_plus_k, one)
        insert_values.append(ast.literal_eval(get_format(datatype, s_val_plus_k)))
        for edge in self.global_join_graph:
            if attrib_inner in edge:
                edge_key = frozenlist.FrozenList(edge)
                edge_key.freeze()
                if edge_key in self.joined_attrib_valDict.keys():
                    self.joined_attrib_valDict[edge_key].append(s_val_plus_k)
                else:
                    self.joined_attrib_valDict[edge_key] = [s_val_plus_k]

    def __get_pre_assignment(self):
        pre_assignment = True
        for elt in self.global_groupby_attributes:
            if elt in self.joined_attribs:
                pre_assignment = False
                break
        if not self.global_groupby_attributes:
            pre_assignment = False
        return pre_assignment
