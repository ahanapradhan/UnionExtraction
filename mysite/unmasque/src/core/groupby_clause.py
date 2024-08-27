import ast

from frozenlist._frozenlist import FrozenList

from .dataclass.genPipeline_context import GenPipelineContext

from ...src.core.abstract.GenerationPipeLineBase import GenerationPipeLineBase, get_boundary_value
from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util.utils import get_dummy_val_for, get_val_plus_delta, get_format, get_char
from ..util.constants import COUNT_THERE, CONST_1_THERE, CONST_1_VALUE, NUMBER_TYPES, NON_TEXT_TYPES


def has_attrib_key_condition(attrib, attrib_inner, key_list):
    return attrib_inner == attrib or attrib_inner in key_list


class GroupBy(GenerationPipeLineBase):
    def __init__(self, connectionHelper: AbstractConnectionHelper,
                 genPipelineCtx: GenPipelineContext,
                 pgao_ctx):
        super().__init__(connectionHelper, "Group By", genPipelineCtx)
        self.projected_attribs = pgao_ctx.projected_attribs
        self.has_groupby = False
        self.group_by_attrib = []

    def doExtractJob(self, query):
        # Array to check for 1 as constant or count in select clause. (will be used later)
        check_array = [0] * len(self.projected_attribs)

        for tabname in self.core_relations:
            attrib_list = self.global_all_attribs[tabname]

            for attrib in attrib_list:
                self.truncate_core_relations()
                self.logger.debug("Checking for attrib: ", attrib)
                # determine offset values for this attribute
                curr_attrib_value = [0, 1, 1]

                key_list = next((elt for elt in self.global_join_graph if attrib in elt), [])

                # For this table (tabname) and this attribute (attrib), fill all tables now
                for tabname_inner in self.core_relations:
                    attrib_list_inner = self.global_all_attribs[tabname_inner]
                    insert_rows = []
                    no_of_rows = 3 if tabname_inner == tabname else 1
                    key_path_flag = any(val in key_list for val in attrib_list_inner)
                    if tabname_inner != tabname and key_path_flag:
                        no_of_rows = 2

                    attrib_list_str = ",".join(attrib_list_inner)
                    att_order = f"({attrib_list_str})"
                    self.logger.debug(f"for {tabname_inner} insert {no_of_rows} rows")
                    for k in range(no_of_rows):
                        insert_values = []
                        for attrib_inner in attrib_list_inner:
                            datatype = self.get_datatype((tabname_inner, attrib_inner))

                            if has_attrib_key_condition(attrib, attrib_inner, key_list):
                                self.insert_values_for_joined_attribs(attrib_inner, curr_attrib_value, datatype,
                                                                      insert_values, k, tabname_inner)
                            else:
                                self.insert_values_for_single_attrib(attrib_inner, datatype, insert_values,
                                                                     tabname_inner)
                        insert_rows.append(tuple(insert_values))

                    self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)

                self.see_d_min()
                new_result = self.app.doJob(query)

                # Checking for 1 as constant in select clause or count is present.
                for i in range(len(self.projected_attribs)):
                    if self.projected_attribs[i] == '':
                        for j in range(1, len(new_result)):  # skipping the header of result
                            if new_result[j][i] != CONST_1_VALUE:
                                check_array[i] = COUNT_THERE
                            elif new_result[j][i] == CONST_1_VALUE and check_array[i] != COUNT_THERE:
                                check_array[i] = CONST_1_THERE

                if self.app.isQ_result_empty(new_result):
                    self.logger.error('some error in generating new database. '
                                      'Result is empty. Can not identify Grouping')
                    return False
                nonEmpty_rows = self.app.get_all_nullfree_rows(new_result)
                if len(nonEmpty_rows) == 2:
                    self.group_by_attrib.append(attrib)
                    self.has_groupby = True
                elif len(nonEmpty_rows) == 1:
                    # It indicates groupby on at least one attribute
                    self.has_groupby = True

        self.remove_duplicates()

        # Putting the value 1 where 1 is there in the result wrt array.
        for i in range(len(check_array)):
            if check_array[i] == CONST_1_THERE:
                self.projected_attribs[i] = CONST_1_VALUE

        return True

    def insert_values_for_single_attrib(self, attrib_inner, datatype, insert_values, tabname_inner):
        if datatype in NON_TEXT_TYPES:
            val = self.get_insert_value_for_single_attrib(datatype, attrib_inner, tabname_inner)
            if datatype == 'date':
                insert_values.append(ast.literal_eval(get_format('date', val)))
            else:
                insert_values.append(get_format('int', val))
        else:
            if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
                filtered_val = self.get_s_val_for_textType(attrib_inner, tabname_inner)
                char_val = filtered_val.replace('%', '')
            else:
                char_val = get_char(get_dummy_val_for('char'))
            insert_values.append(char_val)

    def insert_values_for_joined_attribs(self, attrib_inner, curr_attrib_value, datatype, insert_values, k,
                                         tabname_inner):
        delta = curr_attrib_value[k]
        if datatype in NON_TEXT_TYPES:
            val = self.get_insert_value_for_joined_attribs(datatype, attrib_inner,
                                                           delta, tabname_inner)
            if datatype == 'date':
                insert_values.append(ast.literal_eval(get_format('date', val)))
            else:
                insert_values.append(get_format('int', val))
        else:
            plus_val = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), delta))
            if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
                filtered_val = self.get_s_val_for_textType(attrib_inner, tabname_inner)
                if '_' in filtered_val:
                    insert_values.append(filtered_val.replace('_', plus_val))
                else:
                    insert_values.append(filtered_val.replace('%', plus_val, 1))
                insert_values[-1].replace('%', '')
            else:
                insert_values.append(plus_val)

    def get_insert_value_for_single_attrib(self, datatype, attrib_inner, tabname_inner):
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            val = get_boundary_value(val, is_ub=False)
        else:
            val = get_dummy_val_for(datatype)
        return val

    def get_insert_value_for_joined_attribs(self, datatype, attrib_inner, delta, tabname_inner):
        self.logger.debug(tabname_inner, attrib_inner)
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            val = self.__get_s_plus_k_val(attrib_inner, datatype, delta, tabname_inner)
        else:
            val = get_val_plus_delta(datatype, get_dummy_val_for(datatype), delta)
        self.logger.debug(val)
        return val

    def __get_s_plus_k_val(self, attrib_inner, datatype, delta, tabname_inner):
        if isinstance(self.filter_attrib_dict[(tabname_inner, attrib_inner)], tuple):  # range
            zero_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            zero_val = get_val_plus_delta(datatype, zero_val, delta)
            one_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][1]
            val = min(zero_val, one_val)
        elif isinstance(self.filter_attrib_dict[(tabname_inner, attrib_inner)], FrozenList):  # IN
            zero_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            i = 0
            while i <= delta:
                zero_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][i]
                if not isinstance(zero_val, tuple):
                    i += 1
                    continue
                else:
                    zero_val, zone_val = zero_val[0], zero_val[-1]
                    left = delta - i
                    if datatype == 'date':
                        gap = (zone_val - zero_val).days
                    elif datatype in NUMBER_TYPES:
                        gap = zone_val - zero_val
                    else:
                        raise ValueError
                    if gap >= left:
                        zero_val = get_val_plus_delta(datatype, zero_val, left)
                        break
                    else:
                        i = left - gap
            one_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)][-1]
            one_val = get_boundary_value(one_val, is_ub=True)
            val = min(zero_val, one_val)
        else:  # =
            val = self.filter_attrib_dict[(tabname_inner, attrib_inner)]
        return val

    def remove_duplicates(self):
        to_remove = []
        for attrib in self.group_by_attrib:
            if attrib not in self.projected_attribs:
                to_remove.append(attrib)
        for r in to_remove:
            self.group_by_attrib.remove(r)
        self.group_by_attrib.sort()
