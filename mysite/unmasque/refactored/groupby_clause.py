import ast

from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty, get_val_plus_delta, get_format, get_dummy_val_for, \
    get_char, get_2_elems_sublists


def get_escape_string(att_order, attrib_list_inner):
    esc_string = '(' + '%s'
    for k in range(1, len(attrib_list_inner)):
        esc_string = esc_string + ", " + '%s'
    esc_string = esc_string + ")"
    att_order = att_order[:-1]
    att_order += ')'
    return att_order, esc_string


def has_attrib_key_condition(attrib, attrib_inner, key_list):
    return attrib_inner == attrib or attrib_inner in key_list


class GroupBy(Base):
    def __init__(self, connectionHelper,
                 global_attrib_types,
                 core_relations,
                 filter_predicates,
                 global_all_attribs,
                 join_graph,
                 projected_attribs):
        super().__init__(connectionHelper, "Group by")
        self.app = Executable(connectionHelper)
        self.global_attrib_types = global_attrib_types
        self.core_relations = core_relations
        self.global_filter_predicates = filter_predicates
        self.global_all_attribs = global_all_attribs  # from where clause
        self.global_join_graph = join_graph
        self.projected_attribs = projected_attribs

        self.has_groupby = False
        self.group_by_attrib = []

    def truncate_core_relations(self):
        # Truncate all core relations
        for table in self.core_relations:
            self.connectionHelper.execute_sql(["Truncate Table " + table + ";"])

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}

        filter_attrib_dict = self.construct_filter_attribs_dict()

        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            attrib_list = self.global_all_attribs[i]

            for attrib in attrib_list:
                self.truncate_core_relations()

                # determine offset values for this attribute
                curr_attrib_value = [0, 1, 1]

                key_list = next((elt for elt in self.global_join_graph if attrib in elt), [])

                # For this table (tabname) and this attribute (attrib), fill all tables now
                for j in range(len(self.core_relations)):
                    tabname_inner = self.core_relations[j]
                    attrib_list_inner = self.global_all_attribs[j]

                    insert_rows = []

                    no_of_rows = 3 if tabname_inner == tabname else 1
                    key_path_flag = any(val in key_list for val in attrib_list_inner)
                    if tabname_inner != tabname and key_path_flag:
                        no_of_rows = 2

                    att_order = '('
                    flag = False
                    for k in range(no_of_rows):
                        insert_values = []
                        for attrib_inner in attrib_list_inner:
                            if not flag:
                                att_order += attrib_inner + ","
                            if has_attrib_key_condition(attrib, attrib_inner, key_list):
                                delta = curr_attrib_value[k]
                                if 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
                                    if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                        zero_date = get_val_plus_delta('date',
                                                                       filter_attrib_dict[
                                                                           (tabname_inner, attrib_inner)][0], delta)
                                        one_date = filter_attrib_dict[(tabname_inner, attrib_inner)][1]
                                        date_val = min(zero_date, one_date)
                                    else:
                                        date_val = get_val_plus_delta('date', get_dummy_val_for('date'), delta)
                                    insert_values.append(ast.literal_eval(get_format('date', date_val)))
                                elif ('int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in
                                      attrib_types_dict[(tabname_inner, attrib_inner)]):
                                    # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
                                    if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                        zero_val = get_val_plus_delta('int',
                                                                      filter_attrib_dict[(tabname_inner, attrib_inner)][
                                                                          0], delta)
                                        one_val = filter_attrib_dict[(tabname_inner, attrib_inner)][1]
                                        number_val = min(zero_val, one_val)
                                    else:
                                        number_val = get_val_plus_delta('int', get_dummy_val_for('int'), delta)
                                    insert_values.append(get_format('int', number_val))
                                else:
                                    plus_val = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), delta))
                                    if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                        if '_' in filter_attrib_dict[(tabname_inner, attrib_inner)]:
                                            insert_values.append(
                                                filter_attrib_dict[(tabname_inner, attrib_inner)].replace('_',
                                                                                                          plus_val))
                                        else:
                                            insert_values.append(
                                                filter_attrib_dict[(tabname_inner, attrib_inner)].replace('%', plus_val,
                                                                                                          1))
                                        insert_values[-1].replace('%', '')
                                    else:
                                        insert_values.append(plus_val)
                            else:
                                if 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
                                    if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                        date_val = filter_attrib_dict[(tabname_inner, attrib_inner)][0]
                                    else:
                                        date_val = get_dummy_val_for('date')
                                    insert_values.append(ast.literal_eval(get_format('date', date_val)))
                                elif 'int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in \
                                        attrib_types_dict[(tabname_inner, attrib_inner)]:
                                    # check for filter
                                    if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                        number_val = filter_attrib_dict[(tabname_inner, attrib_inner)][0]
                                    else:
                                        number_val = get_dummy_val_for('int')
                                    insert_values.append(number_val)
                                else:
                                    if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
                                        char_val = filter_attrib_dict[(tabname_inner, attrib_inner)].replace('%', '')
                                    else:
                                        char_val = get_char(get_dummy_val_for('char'))
                                    insert_values.append(char_val)
                        flag = True
                        insert_rows.append(tuple(insert_values))

                    att_order, esc_string = get_escape_string(att_order, attrib_list_inner)
                    insert_query = "INSERT INTO " + tabname_inner + att_order + " VALUES " + esc_string
                    self.connectionHelper.execute_sql_with_params(insert_query, insert_rows)

                new_result = self.app.doJob(query)

                if isQ_result_empty(new_result):
                    print('some error in generating new database. Result is empty. Can not identify Grouping')
                    return False
                elif len(new_result) == 3:
                    # 3 is WITH HEADER so it is checking for two rows
                    self.group_by_attrib.append(attrib)
                    self.has_groupby = True
                elif len(new_result) == 2:
                    # It indicates groupby on at least one attribute
                    self.has_groupby = True

        self.remove_duplicates()
        return True

    def remove_duplicates(self):
        to_remove = []
        for attrib in self.group_by_attrib:
            if attrib not in self.projected_attribs:
                to_remove.append(attrib)
        for r in to_remove:
            self.group_by_attrib.remove(r)

    def construct_filter_attribs_dict(self):
        # get filter values and their allowed minimum and maximum value
        filter_attrib_dict = {}
        for entry in self.global_filter_predicates:
            if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
                filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
            else:
                filter_attrib_dict[(entry[0], entry[1])] = entry[3]
        return filter_attrib_dict
