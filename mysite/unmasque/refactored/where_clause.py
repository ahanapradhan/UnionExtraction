import copy
import math

from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import is_int, get_all_combo_lists, get_datatype_from_typesList, \
    get_dummy_val_for, get_val_plus_delta, get_min_and_max_val, isQ_result_empty, is_left_less_than_right_by_cutoff, \
    get_format, get_mid_val, get_cast_value


def get_two_different_vals(list_type):
    datatype = get_datatype_from_typesList(list_type)
    val1 = get_dummy_val_for(datatype)
    val2 = get_val_plus_delta(datatype, val1, 1)
    return val1, val2


def construct_two_lists(attrib_types_dict, curr_list, elt):
    list1 = [curr_list[index] for index in elt]
    list_type = attrib_types_dict[curr_list[elt[0]]] if elt else ''
    list2 = list(set(curr_list) - set(list1))
    return list1, list2, list_type


def get_constants_for(datatype):
    if datatype == 'int' or datatype == 'date':
        while_cut_off = 0
        delta = 1
    elif datatype == 'float' or datatype == 'numeric':
        while_cut_off = 0.00001
        delta = 0.01
    return delta, while_cut_off


class WhereClause(Base):
    local_other_info_dict = {}
    local_instance_no = 0
    local_instance_list = []

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_other_info_dict,
                 global_result_dict,
                 global_min_instance_dict):
        super().__init__(connectionHelper, "Where_clause")
        self.filter_predicates = None
        self.app = Executable(connectionHelper)

        # from initiator
        self.global_key_lists = global_key_lists

        # from from clause
        self.core_relations = core_relations

        # from view minimizer
        self.global_other_info_dict = global_other_info_dict
        self.global_min_instance_dict = global_min_instance_dict
        self.global_result_dict = global_result_dict

        # init data
        self.global_attrib_types = []
        self.global_all_attribs = []
        self.global_d_plus_value = {}  # this is the tuple from D_min
        self.global_attrib_max_length = {}

        # join data
        self.global_attrib_types_dict = {}
        self.global_attrib_dict = {}
        self.global_join_instance_dict = {}
        self.global_component_dict = {}
        self.global_join_graph = []
        self.global_key_attributes = []

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        self.get_init_data()
        query = self.extract_params_from_args(args)
        self.get_join_graph(query)
        self.filter_predicates = self.get_filter_predicates(query)
        return self.global_join_graph, self.filter_predicates

    def get_join_graph(self, query):
        global_key_lists = copy.deepcopy(self.global_key_lists)
        join_graph = []
        attrib_types_dict, combo_dict_of_lists = self.construct_attribs_types_dict()

        # For each list, test its presence in join graph
        # This will either add the list in join graph or break it
        self.global_attrib_dict['join'] = []
        k = 0
        while global_key_lists:
            curr_list = global_key_lists[0]
            join_keys = [join_key for join_key in curr_list if join_key[0] in self.core_relations]
            if len(join_keys) <= 1:
                global_key_lists.remove(curr_list)
                continue
            print("... checking for: ", join_keys)

            k += 1
            self.global_attrib_dict['join'].append("Component-" + str(k))
            self.global_join_instance_dict['Component-' + str(k)] = []
            self.global_component_dict['Component-' + str(k)] = join_keys

            # Try for all possible combinations
            for elt in combo_dict_of_lists[len(join_keys)]:
                self.local_other_info_dict = {}

                list1, list2, list_type = construct_two_lists(attrib_types_dict, join_keys, elt)
                val1, val2 = get_two_different_vals(list_type)
                temp_copy = {tab: self.global_min_instance_dict[tab] for tab in self.core_relations}

                # Assign two different values to two lists in database
                self.assign_values_to_lists(list1, list2, temp_copy, val1, val2)

                self.fill_join_dicts_for_demo(k, list1, list2, temp_copy, val1, val2)

                # CHECK THE RESULT
                new_result = self.app.doJob(query)
                self.global_result_dict['join_' + self.global_attrib_dict['join'][-1] + '_' +
                                        self.global_join_instance_dict['Component-' + str(k)][-1]] = new_result
                self.local_other_info_dict['Result Cardinality'] = len(new_result) - 1
                if len(new_result) > 1:
                    self.remove_edge_from_join_graph_dicts(join_keys, list1, list2, global_key_lists)
                    break

            for keys in global_key_lists:
                if all(x in keys for x in join_keys):
                    global_key_lists.remove(keys)
                    join_graph.append(copy.deepcopy(join_keys))
                    self.local_other_info_dict['Conclusion'] = u'Edge ' + list1[0][1] + u"\u2014" + list2[0][
                        1] + ' is present in the join graph'

            # Assign same values in all cur_lists to get non-empty output
            self.global_other_info_dict['join_' + self.global_attrib_dict['join'][-1] + '_' +
                                        self.global_join_instance_dict['Component-' + str(k)][
                                            -1]] = copy.deepcopy(self.local_other_info_dict)
            for val in join_keys:
                self.connectionHelper.execute_sql(["Insert into " + val[0] + " Select * from " + val[0] + "4;"])

        self.refine_join_graph(join_graph)
        return

    def remove_edge_from_join_graph_dicts(self, curr_list, list1, list2, global_key_lists):
        self.local_other_info_dict[
            'Conclusion'] = 'Selected edge(s) are not present in the join graph'
        for keys in global_key_lists:
            if all(x in keys for x in curr_list):
                global_key_lists.remove(keys)
        global_key_lists.append(copy.deepcopy(list1))
        global_key_lists.append(copy.deepcopy(list2))

    def fill_join_dicts_for_demo(self, k, list1, list2, temp_copy, val1, val2):
        # Hardcoding for demo, need to be revised
        self.global_join_instance_dict['Component-' + str(k)].append(
            u"" + list1[0][1] + u"\u2014" + list2[0][1])
        self.local_other_info_dict['Current Mutation'] = 'Mutation of ' + list1[0][
            1] + ' with value ' + str(val1) + " and " + list2[0][1] + ' with value ' + str(val2)
        for tabname in self.core_relations:
            self.global_min_instance_dict[
                'join_' + self.global_attrib_dict['join'][-1] + '_' + tabname + '_' +
                self.global_join_instance_dict['Component-' + str(k)][-1]] = copy.deepcopy(
                temp_copy[tabname])
        ########################################

    def assign_values_to_lists(self, list1, list2, temp_copy, val1, val2):
        self.assign_value_to_list(list1, temp_copy, val1)
        self.assign_value_to_list(list2, temp_copy, val2)

    def assign_value_to_list(self, list1, temp_copy, val1):
        for val in list1:
            self.connectionHelper.execute_sql(
                ["update " + str(val[0]) + " set " + str(val[1]) + " = " + str(val1) + ";"])
            index = temp_copy[val[0]][0].index(val[1])
            mutated_list = copy.deepcopy(list(temp_copy[val[0]][1]))
            mutated_list[index] = str(val1)
            temp_copy[val[0]][1] = tuple(mutated_list)

    def construct_attribs_types_dict(self):
        max_list_len = max(len(elt) for elt in self.global_key_lists)
        combo_dict_of_lists = get_all_combo_lists(max_list_len)
        attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        return attrib_types_dict, combo_dict_of_lists

    def refine_join_graph(self, join_graph):
        # refine join graph and get all key attributes
        self.global_join_graph = []
        self.global_key_attributes = []
        for elt in join_graph:
            temp = []
            for val in elt:
                temp.append(val[1])
                self.global_key_attributes.append(val[1])
            self.global_join_graph.append(copy.deepcopy(temp))

    def get_init_data(self):
        if len(self.global_attrib_types) + len(self.global_all_attribs) + len(self.global_d_plus_value) + len(
                self.global_attrib_max_length) == 0:
            self.do_init()

    def do_init(self):
        for tabname in self.core_relations:
            res, desc = self.connectionHelper.execute_sql_fetchall("select column_name, data_type, "
                                                                   "character_maximum_length from "
                                                                   "information_schema.columns "
                                                                   "where table_schema = 'public' and "
                                                                   "table_name = '" + tabname + "';")
            tab_attribs = []
            tab_attribs.extend(row[0] for row in res)
            self.global_all_attribs.append(copy.deepcopy(tab_attribs))

            self.global_attrib_types.extend((tabname, row[0], row[1]) for row in res)

            self.global_attrib_max_length.update(
                {(tabname, row[0]): int(str(row[2])) for row in res if is_int(str(row[2]))})

            res, desc = self.connectionHelper.execute_sql_fetchall("select "
                                                                   + ", ".join(tab_attribs)
                                                                   + " from " + tabname + ";")
            for row in res:
                for attrib, value in zip(tab_attribs, row):
                    self.global_d_plus_value[attrib] = value

    def get_filter_predicates(self, query):
        # query = self.extract_params_from_args(args)

        self.global_attrib_dict['filter'] = []
        self.do_init()

        filter_attribs = []
        total_attribs = 0
        d_plus_value = copy.deepcopy(self.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.global_attrib_max_length)

        for entry in self.global_attrib_types:
            # attrib_types_dict[(entry[0], entry[1])] = entry[2]
            # aoa change
            self.global_attrib_types_dict[(entry[0], entry[1])] = entry[2]

        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            attrib_list = self.global_all_attribs[i]
            total_attribs = total_attribs + len(attrib_list)
            for attrib in attrib_list:
                if attrib not in self.global_key_attributes:  # filter is allowed only on non-key attribs
                    self.extract_filter_on_attrib(attrib, attrib_max_length, d_plus_value, filter_attribs,
                                                  query, tabname)

                    print("filter_attribs", filter_attribs)
        return filter_attribs

    def extract_filter_on_attrib(self, attrib, attrib_max_length, d_plus_value, filter_attribs, query, tabname):
        """
        self.global_attrib_dict['filter'].append(attrib)
        self.local_other_info_dict = {}
        self.local_instance_no = 1
        self.global_instance_dict[attrib] = []
        self.local_instance_list = []
        """

        if 'int' in self.global_attrib_types_dict[(tabname, attrib)]:
            self.handle_date_or_int_filter('int', attrib, d_plus_value, filter_attribs, tabname, query)

        elif 'date' in self.global_attrib_types_dict[(tabname, attrib)]:
            self.handle_date_or_int_filter('date', attrib, d_plus_value, filter_attribs, tabname, query)

        elif any(x in self.global_attrib_types_dict[(tabname, attrib)] for x in ['text', 'char', 'varbit']):
            self.handle_string_filter(attrib, attrib_max_length, d_plus_value, filter_attribs, tabname, query)

        elif 'numeric' in self.global_attrib_types_dict[(tabname, attrib)]:
            self.handle_numeric_filter(attrib, d_plus_value, filter_attribs, tabname, query)

        # self.global_instance_dict['filter_' + attrib] = copy.deepcopy(self.local_instance_list)

    def checkAttribValueEffect(self, query, tabname, attrib, val):
        self.connectionHelper.execute_sql(["update " + tabname + " set " + attrib + " = " + str(val) + ";"])
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return False
        return True


    def handle_numeric_filter(self, attrib, d_plus_value, filterAttribs, tabname, query):
        min_val_domain, max_val_domain = get_min_and_max_val('numeric')
        # NUMERIC HANDLING
        # PRECISION TO BE GET FROM SCHEMA GRAPH
        min_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  min_val_domain)  # True implies row was still present
        max_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  max_val_domain)  # True implies row was still present
        # inference based on flag_min and flag_max
        if not min_present and not max_present:
            equalto_flag = self.get_filter_value(query, 'int', tabname, attrib, float(d_plus_value[attrib]) - .01,
                                                 float(d_plus_value[attrib]) + .01, '=')
            if equalto_flag:
                filterAttribs.append(
                    (tabname, attrib, '=', float(d_plus_value[attrib]), float(d_plus_value[attrib])))
            else:
                val1 = self.get_filter_value(query, 'float', tabname, attrib, math.ceil(float(d_plus_value[attrib])),
                                             max_val_domain, '<=')
                val2 = self.get_filter_value(query, 'float', tabname, attrib, min_val_domain,
                                             math.floor(float(d_plus_value[attrib])), '>=')
                filterAttribs.append((tabname, attrib, 'range', float(val2), float(val1)))
        elif min_present and not max_present:
            val = self.get_filter_value(query, 'float', tabname, attrib, math.ceil(float(d_plus_value[attrib])) - 5,
                                        max_val_domain, '<=')
            val = float(val)
            val1 = self.get_filter_value(query, 'float', tabname, attrib, val, val + 0.99, '<=')
            filterAttribs.append((tabname, attrib, '<=', float(min_val_domain), float(round(val1, 2))))
        elif not min_present and max_present:
            val = self.get_filter_value(query, 'float', tabname, attrib, min_val_domain,
                                        math.floor(float(d_plus_value[attrib]) + 5), '>=')
            val = float(val)
            val1 = self.get_filter_value(query, 'float', tabname, attrib, val - 1, val, '>=')
            filterAttribs.append((tabname, attrib, '>=', float(round(val1, 2)), float(max_val_domain)))

    def get_filter_value(self, query, datatype,
                         tabname, filter_attrib,
                         min_val, max_val, operator):
        query_front = "update " + str(tabname) + " set " + str(filter_attrib) + " = "
        query_back = ";"
        delta, while_cut_off = get_constants_for(datatype)

        self.revert_filter_changes(tabname)

        low = min_val
        high = max_val

        if operator == '<=':
            while is_left_less_than_right_by_cutoff(datatype, low, high, while_cut_off):
                mid_val, new_result = self.run_app_with_mid_val(datatype, high, low, query, query_front, query_back)
                if mid_val == low or mid_val == high:
                    self.revert_filter_changes(tabname)
                    break
                if isQ_result_empty(new_result):
                    new_val = get_val_plus_delta(datatype, mid_val, -1 * delta)
                    high = new_val
                else:
                    low = mid_val
                self.revert_filter_changes(tabname)
            return low

        if operator == '>=':
            while is_left_less_than_right_by_cutoff(datatype, low, high, while_cut_off):
                mid_val, new_result = self.run_app_with_mid_val(datatype, high, low, query, query_front, query_back)
                if mid_val == low or mid_val == high:
                    self.revert_filter_changes(tabname)
                    break
                if isQ_result_empty(new_result):
                    new_val = get_val_plus_delta(datatype, mid_val, delta)
                    low = new_val
                else:
                    high = mid_val
                self.revert_filter_changes(tabname)
            return high

        else:  # =, i.e. datatype == 'int', date
            is_low = True
            is_high = True
            # updatequery
            is_low = self.run_app_for_a_val(datatype, filter_attrib, is_low,
                                            low, query, query_back, query_front,
                                            tabname)
            is_high = self.run_app_for_a_val(datatype, filter_attrib, is_high,
                                             high, query, query_back, query_front,
                                             tabname)
            self.revert_filter_changes(tabname)
            return not is_low and not is_high

    def run_app_for_a_val(self, datatype, filter_attrib, is_low, low, query, query_back, query_front, tabname):
        low_query = query_front + " " + get_format(datatype, low) + " " + query_back
        self.connectionHelper.execute_sql([low_query])
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            is_low = False
        # put filter_
        # self.update_other_data(tabname, filter_attrib, datatype, low, new_result, [])
        return is_low

    def run_app_with_mid_val(self, datatype, high, low, query, q_front, q_back):
        mid_val = get_mid_val(datatype, high, low)
        print("[low,high,mid]", low, high, mid_val)
        # updatequery
        update_query = q_front + " " + get_format(datatype, mid_val) + q_back
        self.connectionHelper.execute_sql([update_query])
        new_result = self.app.doJob(query)
        print(new_result, mid_val)
        return mid_val, new_result

    # mukul
    def handle_date_or_int_filter(self, datatype, attrib, d_plus_value, filterAttribs, tabname, query):
        # min and max domain values (initialize based on data type)
        # PLEASE CONFIRM THAT DATE FORMAT IN DATABASE IS YYYY-MM-DD
        min_val_domain, max_val_domain = get_min_and_max_val(datatype)
        min_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  get_format(datatype, min_val_domain))  # True implies row
        # was still present
        max_present = self.checkAttribValueEffect(query, tabname, attrib,
                                                  get_format(datatype, max_val_domain))  # True implies row
        # was still present
        if not min_present and not max_present:
            equalto_flag = self.get_filter_value(query, datatype, tabname, attrib,
                                                 get_val_plus_delta(datatype,
                                                                    get_cast_value(datatype, d_plus_value[attrib]), -1),
                                                 get_val_plus_delta(datatype,
                                                                    get_cast_value(datatype, d_plus_value[attrib]), 1),
                                                 '=')
            if equalto_flag:
                filterAttribs.append((tabname, attrib, '=', d_plus_value[attrib], d_plus_value[attrib]))
            else:
                val1 = self.get_filter_value(query, datatype, tabname, attrib,
                                             get_cast_value(datatype, d_plus_value[attrib]),
                                             get_val_plus_delta(datatype,
                                                                get_cast_value(datatype, max_val_domain), -1), '<=')
                val2 = self.get_filter_value(query, datatype, tabname, attrib,
                                             get_val_plus_delta(datatype, get_cast_value(datatype, min_val_domain), 1),
                                             get_cast_value(datatype, d_plus_value[attrib]), '>=')
                filterAttribs.append((tabname, attrib, 'range', val2, val1))
        elif min_present and not max_present:
            val = self.get_filter_value(query, datatype, tabname, attrib,
                                        get_cast_value(datatype, d_plus_value[attrib]),
                                        get_val_plus_delta(datatype,
                                                           get_cast_value(datatype, max_val_domain), -1), '<=')
            filterAttribs.append((tabname, attrib, '<=', min_val_domain, val))
        elif not min_present and max_present:
            val = self.get_filter_value(query, datatype, tabname, attrib,
                                        get_val_plus_delta(datatype, get_cast_value(datatype, min_val_domain), 1),
                                        get_cast_value(datatype, d_plus_value[attrib]), '>=')
            filterAttribs.append((tabname, attrib, '>=', val, max_val_domain))

    def handle_string_filter(self, attrib, attrib_max_length, d_plus_value, filterAttribs, tabname, query):
        # STRING HANDLING
        # ESCAPE CHARACTERS IN STRING REMAINING
        if self.checkStringPredicate(query, tabname, attrib):
            # returns true if there is predicate on this string attribute
            representative = str(d_plus_value[attrib])
            max_length = 100000
            if (tabname, attrib) in attrib_max_length.keys():
                max_length = attrib_max_length[(tabname, attrib)]
            val = self.getStrFilterValue(query, tabname, attrib, representative, max_length)
            val = val.strip()
            if '%' in val or '_' in val:
                filterAttribs.append((tabname, attrib, 'LIKE', val, val))
            else:
                filterAttribs.append((tabname, attrib, 'equal', val, val))
        # update table so that result is not empty
        self.revert_filter_changes(tabname)

    def revert_filter_changes(self, tabname):
        self.connectionHelper.execute_sql(["Truncate table " + tabname + ';',
                                           "Insert into " + tabname + " Select * from " + tabname + "4;"])

    def checkStringPredicate(self, query, tabname, attrib):
        # updatequery
        if self.global_d_plus_value[attrib] is not None and self.global_d_plus_value[attrib][0] == 'a':
            val = 'b'
        else:
            val = 'a'
        new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, val)
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return True
        new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, "" "")
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return True
        return False

    def getStrFilterValue(self, query, tabname, attrib, representative, max_length):
        index = 0
        output = ""
        # currently inverted exclaimaination is being used assuming it will not be in the string
        # GET minimal string with _
        while index < len(representative):
            temp = list(representative)
            if temp[index] == 'a':
                temp[index] = 'b'
            else:
                temp[index] = 'a'
            temp = ''.join(temp)
            new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
            if not isQ_result_empty(new_result):
                temp = copy.deepcopy(representative)
                temp = temp[:index] + temp[index + 1:]
                new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
                if not isQ_result_empty(new_result):
                    representative = representative[:index] + representative[index + 1:]
                else:
                    output = output + "_"
                    representative = list(representative)
                    representative[index] = u"\u00A1"
                    representative = ''.join(representative)
                    index = index + 1
            else:
                output = output + representative[index]
                index = index + 1
        if output == '':
            return output
        # GET % positions
        index = 0
        representative = copy.deepcopy(output)
        if len(representative) < max_length:
            output = ""
            while index < len(representative):
                temp = list(representative)
                if temp[index] == 'a':
                    temp.insert(index, 'b')
                else:
                    temp.insert(index, 'a')
                temp = ''.join(temp)
                new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
                if not isQ_result_empty(new_result):
                    output = output + '%'
                output = output + representative[index]
                index = index + 1
            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)
            new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
            if not isQ_result_empty(new_result):
                output = output + '%'
        return output

    def run_updateQ_with_temp_str(self, attrib, query, tabname, temp):
        # updatequery
        up_query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
        self.connectionHelper.execute_sql([up_query])
        new_result = self.app.doJob(query)
        # self.update_other_data(tabname, attrib, 'text', temp, new_result, [])
        return new_result
