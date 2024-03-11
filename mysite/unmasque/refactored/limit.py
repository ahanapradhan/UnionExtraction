import ast
import copy
import itertools

import frozenlist as frozenlist

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .util.utils import isQ_result_empty, get_val_plus_delta, get_format, get_dummy_val_for, get_char


class Limit(GenerationPipeLineBase):

    def __init__(self, connectionHelper, global_groupby_attributes, delivery):
        super().__init__(connectionHelper, "Limit", delivery)
        self.limit = None
        self.global_groupby_attributes = global_groupby_attributes
        self.joined_attrib_valDict = {}
        self.no_rows = 1000

    def construct_filter_dict(self):
        # get filter values and their allowed minimum and maximum value
        _dict = {}
        for entry_key in self.filter_attrib_dict.keys():
            _dict[entry_key[1]] = self.filter_attrib_dict[entry_key]
        return _dict

    def doExtractJob(self, query):
        grouping_attribute_values = {}

        # notable_filter_attrib_dict = self.construct_filter_dict()
        # notable_attrib_type_dict = self.construct_types_dict()
        pre_assignment = self.get_pre_assignment()

        gb_tab_attribs = [(self.find_tabname_for_given_attrib(attrib), attrib)
                          for attrib in self.global_groupby_attributes]

        total_combinations = 1
        if pre_assignment:
            # GET LIMITS FOR ALL GROUPBY ATTRIBUTES
            group_lists = []
            for elt in gb_tab_attribs:
                temp = []
                if elt not in self.filter_attrib_dict.keys():
                    pre_assignment = False
                    break
                datatype = self.get_datatype(elt)
                if datatype in ['date', 'int', 'numeric']:
                    tot_values = self.compute_total_values(datatype, elt, total_combinations)
                    self.get_temp_total_values(datatype, elt, temp, tot_values)
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

        for j in range(len(self.core_relations)):
            tabname_inner = self.core_relations[j]
            attrib_list_inner = self.global_all_attribs[j]
            attrib_list_str = ",".join(attrib_list_inner)
            att_order = f"({attrib_list_str})"
            insert_rows = []
            for k in range(self.no_rows):
                insert_values = []
                for attrib_inner in attrib_list_inner:
                    datatype = self.get_datatype((tabname_inner, attrib_inner))
                    if attrib_inner in grouping_attribute_values.keys():
                        insert_values.append(grouping_attribute_values[attrib_inner][k])
                    elif attrib_inner not in self.joined_attribs \
                            and (tabname_inner, attrib_inner) not in gb_tab_attribs:
                        insert_values.append(self.get_dmin_val(attrib_inner, tabname_inner))
                    elif datatype in ['date', 'int', 'numeric']:
                        self.insert_non_text_attrib(datatype, attrib_inner, insert_values, k, tabname_inner)
                    else:
                        self.insert_text_attrib(attrib_inner, insert_values, k, tabname_inner)
                insert_rows.append(tuple(insert_values))

            self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)

        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            self.logger.error('some error in generating new database. Result is empty. Can not identify Limit.')
            return False
        else:
            if 4 <= len(new_result) <= 1000:
                self.limit = len(new_result) - 1
            return True

    def get_temp_total_values(self, datatype, elt, temp, tot_values):
        if datatype == 'date':
            for k in range(tot_values):
                date_val = get_val_plus_delta('date', self.filter_attrib_dict[elt][0], k)
                temp.append(ast.literal_eval(get_format('date', date_val)))
        else:
            for k in range(tot_values):
                temp.append(self.filter_attrib_dict[elt][0] + k)

    def compute_total_values(self, datatype, elt, total_combinations):
        if datatype == 'date':
            tot_values = (self.filter_attrib_dict[elt][1] - self.filter_attrib_dict[elt][0]).days + 1
        else:
            tot_values = self.filter_attrib_dict[elt][1] - self.filter_attrib_dict[elt][0] + 1
        if (total_combinations * tot_values) > 1000:
            i = 1
            while (total_combinations * i) < 1001 and i < tot_values:
                i = i + 1
            tot_values = i
        return tot_values

    def insert_text_attrib(self, attrib_inner, insert_values, k, tabname_inner):
        char_val = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), k))
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

    def get_pre_assignment(self):
        pre_assignment = True
        for elt in self.global_groupby_attributes:
            if elt in self.joined_attribs:
                pre_assignment = False
                break
        if not self.global_groupby_attributes:
            pre_assignment = False
        return pre_assignment

    def construct_types_dict(self):
        _dict = {}
        for entry_key in self.attrib_types_dict:
            _dict[entry_key[1]] = self.attrib_types_dict[entry_key]
        return _dict
