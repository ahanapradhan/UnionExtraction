import ast
import copy
import itertools

from .abstract.GenerationPipeLineBase import GenerationPipeLineBase
from .util.utils import isQ_result_empty, get_val_plus_delta, get_format, get_dummy_val_for, get_char


class Limit(GenerationPipeLineBase):

    def __init__(self, connectionHelper, global_attrib_types, global_key_attributes, core_relations, filter_predicates,
                 global_all_attribs, global_groupby_attributes, global_min_instance_dict):
        super().__init__(connectionHelper, "Limit", core_relations, global_all_attribs, global_attrib_types, None,
                         filter_predicates, global_min_instance_dict)
        self.limit = None
        self.global_groupby_attributes = global_groupby_attributes
        self.global_key_attributes = global_key_attributes

    def construct_filter_attribsOnly_dict(self):
        # get filter values and their allowed minimum and maximum value
        filter_onlyattrib_dict = {}
        for entry in self.global_filter_predicates:
            if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
                filter_onlyattrib_dict[entry[1]] = (entry[3], entry[4])
            else:
                filter_onlyattrib_dict[entry[1]] = entry[3]
        return filter_onlyattrib_dict

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        filter_attrib_dict = self.construct_filter_attribs_dict()
        check = self.doExtractJob(query, attrib_types_dict, filter_attrib_dict)
        return check

    def doExtractJob(self, query, attrib_types_dict, filter_attrib_dict):
        grouping_attribute_values = {}

        filter_onlyattrib_dict = self.construct_filter_attribsOnly_dict()

        attribonly_types_dict = self.construct_attributeOnly_types_dict(attrib_types_dict)

        pre_assignment = self.get_pre_assignment()

        total_combinations = 1
        if pre_assignment:
            # GET LIMITS FOR ALL GROUPBY ATTRIBUTES
            group_lists = []
            for elt in self.global_groupby_attributes:
                temp = []
                if elt not in filter_onlyattrib_dict.keys():
                    pre_assignment = False
                    break
                if ('int' in attribonly_types_dict[elt] or 'numeric' in attribonly_types_dict[elt] or 'date' in
                        attribonly_types_dict[elt]):
                    if 'date' in attribonly_types_dict[elt]:
                        tot_values = (filter_onlyattrib_dict[elt][1] - filter_onlyattrib_dict[elt][0]).days + 1
                    else:
                        tot_values = filter_onlyattrib_dict[elt][1] - filter_onlyattrib_dict[elt][0] + 1
                    if (total_combinations * tot_values) > 1000:
                        i = 1
                        while (total_combinations * i) < 1001 and i < tot_values:
                            i = i + 1
                        tot_values = i
                    if 'date' in attribonly_types_dict[elt]:
                        for k in range(tot_values):
                            date_val = get_val_plus_delta('date', filter_onlyattrib_dict[elt][0], k)
                            temp.append(ast.literal_eval(get_format('date', date_val)))
                    else:
                        for k in range(tot_values):
                            temp.append(filter_onlyattrib_dict[elt][0] + k)
                else:
                    if '%' in filter_onlyattrib_dict[elt] or '_' in filter_onlyattrib_dict[elt]:
                        pre_assignment = False
                        break
                    else:
                        temp = [filter_onlyattrib_dict[elt]]
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

        no_of_rows = 1000
        if pre_assignment:
            no_of_rows = min(no_of_rows, total_combinations)

        for j in range(len(self.core_relations)):
            tabname_inner = self.core_relations[j]
            attrib_list_inner = self.global_all_attribs[j]
            att_order = '('
            flag = False
            insert_rows = []
            for k in range(no_of_rows):
                insert_values = []
                for attrib_inner in attrib_list_inner:
                    if not flag:
                        att_order += attrib_inner + ","
                    if attrib_inner in grouping_attribute_values.keys():
                        insert_values.append(grouping_attribute_values[attrib_inner][k])
                    elif 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
                        if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                            zero_k = get_val_plus_delta('date',
                                                        filter_attrib_dict[(tabname_inner, attrib_inner)][0], k)
                            one = filter_attrib_dict[(tabname_inner, attrib_inner)][1]
                            date_val = min(zero_k, one)
                        else:
                            date_val = get_val_plus_delta('date', get_dummy_val_for('date'), k)
                        insert_values.append(ast.literal_eval(get_format('date', date_val)))

                    elif ('int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in
                          attrib_types_dict[(tabname_inner, attrib_inner)]):
                        # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
                        if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                            zero_k = get_val_plus_delta('int',
                                                        filter_attrib_dict[(tabname_inner, attrib_inner)][0], k)
                            one = filter_attrib_dict[(tabname_inner, attrib_inner)][1]
                            date_val = min(zero_k, one)
                        else:
                            date_val = get_val_plus_delta('int', get_dummy_val_for('int'), k)
                        insert_values.append(ast.literal_eval(get_format('int', date_val)))
                    else:
                        char_val = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), k))
                        if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                            temp = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
                            if '_' in filter_attrib_dict[(tabname_inner, attrib_inner)]:
                                insert_values.append(temp.replace('_', char_val))
                            else:
                                insert_values.append(temp.replace('%', char_val, 1))
                            insert_values[-1].replace('%', '')
                        else:
                            insert_values.append(char_val)
                flag = True
                insert_rows.append(tuple(insert_values))

            self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)

        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            self.logger.error('some error in generating new database. Result is empty. Can not identify Limit.')
            return False
        else:
            if len(new_result) <= 1000:
                self.limit = len(new_result) - 1
            return True

    def get_pre_assignment(self):
        pre_assignment = True
        for elt in self.global_groupby_attributes:
            if elt in self.global_key_attributes:
                pre_assignment = False
                break
        if not self.global_groupby_attributes:
            pre_assignment = False
        return pre_assignment

    def construct_attributeOnly_types_dict(self, attrib_types_dict):
        attribonly_types_dict = {}
        for entry in self.global_attrib_types:
            attrib_types_dict[(entry[0], entry[1])] = entry[2]
            attribonly_types_dict[entry[1]] = entry[2]
        return attribonly_types_dict
