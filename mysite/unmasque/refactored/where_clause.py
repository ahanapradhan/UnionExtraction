import copy
import datetime

from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import is_int, get_all_combo_lists, get_datatype_from_typesList, \
    get_dummy_val_for, get_val_plus_delta


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

        self.global_instance_dict = {}

    def extract_params_from_args(self, args):
        return args[0]

    def get_join_graph(self, query):
        # self.get_init_data()
        global_key_lists = self.global_key_lists
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

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)

        self.global_attrib_dict['filter'] = []
        self.get_init_data()
        filterAttribs = []
        total_attribs = 0
        # attrib_types_dict = {}
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
                if attrib not in self.global_key_attributes:
                    self.global_attrib_dict['filter'].append(attrib)
                    self.local_other_info_dict = {}
                    self.local_instance_no = 1
                    self.global_instance_dict[attrib] = []
                    self.local_instance_list = []
                    if 'int' in self.global_attrib_types_dict[(tabname, attrib)]:
                        self.handle_int_filter(attrib, d_plus_value, filterAttribs, tabname)
                    elif any(x in self.global_attrib_types_dict[(tabname, attrib)] for x in ['text', 'char', 'varbit']):
                        self.handle_string_filter(attrib, attrib_max_length, d_plus_value, filterAttribs, tabname)
                    elif 'date' in self.global_attrib_types_dict[(tabname, attrib)]:
                        self.handle_date_filter(attrib, d_plus_value, filterAttribs, tabname)
                    elif 'numeric' in self.global_attrib_types_dict[(tabname, attrib)]:
                        self.handle_numeric_filter(attrib, d_plus_value, filterAttribs, tabname)
                    self.global_instance_dict['filter_' + attrib] = copy.deepcopy(
                        self.local_instance_list)
        print("filterAttribs", filterAttribs)
        return filterAttribs

    def handle_numeric_filter(self, attrib, d_plus_value, filterAttribs, tabname):
        pass
        """

        # NUMERIC HANDLING
        # min and max domain values (initialize based on data type)
        min_val_domain = -214748364888
        max_val_domain = 214748364788
        # PRECISION TO BE GET FROM SCHEMA GRAPH
        precision = 2
        flag_min = checkAttribValueEffect(tabname, attrib,
                                          min_val_domain)  # True implies row was still present
        flag_max = checkAttribValueEffect(tabname, attrib,
                                          max_val_domain)  # True implies row was still present
        # inference based on flag_min and flag_max
        if (flag_max == True and flag_min == True):
            self.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
        elif (flag_min == False and flag_max == False):
            print('identifying value for Int filter(range) attribute..', attrib)
            equalto_flag = getIntFilterValue(tabname, attrib, float(d_plus_value[attrib]) - .01,
                                             float(d_plus_value[attrib]) + .01, '=')
            if equalto_flag:
                filterAttribs.append(
                    (tabname, attrib, '=', float(d_plus_value[attrib]), float(d_plus_value[attrib])))
            else:
                val1 = getIntFilterValue(tabname, attrib, math.ceil(float(d_plus_value[attrib])),
                                         max_val_domain, '<=')
                val2 = getIntFilterValue(tabname, attrib, min_val_domain,
                                         math.floor(float(d_plus_value[attrib])), '>=')
                filterAttribs.append((tabname, attrib, 'range', float(val2), float(val1)))
        elif (flag_min == True and flag_max == False):
            print('identifying value for Int filter attribute', attrib)
            val = getIntFilterValue(tabname, attrib, math.ceil(float(d_plus_value[attrib])) - 5,
                                    max_val_domain, '<=')
            val = float(val)
            val1 = getFloatFilterValue(tabname, attrib, val, val + 0.99, '<=')
            filterAttribs.append((tabname, attrib, '<=', float(min_val_domain), float(round(val1, 2))))
        elif (flag_min == False and flag_max == True):
            print('identifying value for Int filter attribute', attrib)
            val = getIntFilterValue(tabname, attrib, min_val_domain,
                                    math.floor(float(d_plus_value[attrib]) + 5), '>=')
            val = float(val)
            val1 = getFloatFilterValue(tabname, attrib, val - 1, val, '>=')
            filterAttribs.append((tabname, attrib, '>=', float(round(val1, 2)), float(max_val_domain)))
    """

    def handle_date_filter(self, attrib, d_plus_value, filterAttribs, tabname):
        pass
        """

        # min and max domain values (initialize based on data type)
        # PLEASE CONFIRM THAT DATE FORMAT IN DATABASE IS YYYY-MM-DD
        min_val_domain = datetime.date(1, 1, 1)
        max_val_domain = datetime.date(9999, 12, 31)
        flag_min = checkAttribValueEffect(tabname, attrib, "'" + str(
            min_val_domain) + "'")  # True implies row was still present
        flag_max = checkAttribValueEffect(tabname, attrib, "'" + str(
            max_val_domain) + "'")  # True implies row was still present
        # inference based on flag_min and flag_max
        if (flag_max == True and flag_min == True):
            self.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
        elif (flag_min == False and flag_max == False):
            self.local_other_info_dict[
                'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator between '
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for Date filter(range) attribute..', attrib)
            equalto_flag = getDateFilterValue(tabname, attrib,
                                              d_plus_value[attrib] - datetime.timedelta(days=1),
                                              d_plus_value[attrib] + datetime.timedelta(days=1), '=')
            if equalto_flag:
                filterAttribs.append((tabname, attrib, '=', d_plus_value[attrib], d_plus_value[attrib]))
                self.local_other_info_dict[
                    'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' = ' + str(
                    d_plus_value[attrib])
                self.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                    self.local_instance_no - 1)] = copy.deepcopy(
                    self.local_other_info_dict)
            else:
                val1 = getDateFilterValue(tabname, attrib, d_plus_value[attrib],
                                          max_val_domain - datetime.timedelta(days=1), '<=')
                val2 = getDateFilterValue(tabname, attrib, min_val_domain + datetime.timedelta(days=1),
                                          d_plus_value[attrib], '>=')
                filterAttribs.append((tabname, attrib, 'range', val2, val1))
                self.local_other_info_dict[
                    'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' between ' + str(
                    val2) + ' and ' + str(val1)
                self.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                    self.local_instance_no - 1)] = copy.deepcopy(
                    self.local_other_info_dict)
        elif (flag_min == True and flag_max == False):
            self.local_other_info_dict[
                'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator <='
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for Date filter attribute', attrib)
            val = getDateFilterValue(tabname, attrib, d_plus_value[attrib],
                                     max_val_domain - datetime.timedelta(days=1), '<=')
            filterAttribs.append((tabname, attrib, '<=', min_val_domain, val))
            self.local_other_info_dict[
                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' <= ' + str(val)
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
        elif (flag_min == False and flag_max == True):
            self.local_other_info_dict[
                'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator >= '
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for Date filter attribute', attrib)
            val = getDateFilterValue(tabname, attrib, min_val_domain + datetime.timedelta(days=1),
                                     d_plus_value[attrib], '>=')
            filterAttribs.append((tabname, attrib, '>=', val, max_val_domain))
            self.local_other_info_dict[
                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' >= ' + str(val)
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
                    """

    def handle_string_filter(self, attrib, attrib_max_length, d_plus_value, filterAttribs, tabname):
        pass
        """

        # STRING HANDLING
        # ESCAPE CHARACTERS IN STRING REMAINING
        if (checkStringPredicate(tabname, attrib)):
            # returns true if there is predicate on this string attribute
            self.local_other_info_dict['Conclusion'] = 'Filter Predicate on ' + attrib
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for String filter attribute', attrib)
            representative = str(d_plus_value[attrib])
            max_length = 100000
            if (tabname, attrib) in attrib_max_length.keys():
                max_length = attrib_max_length[(tabname, attrib)]
            val = getStrFilterValue(tabname, attrib, representative, max_length)
            if ('%' in val or '_' in val):
                filterAttribs.append((tabname, attrib, 'LIKE', val, val))
                self.local_other_info_dict[
                    'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' LIKE ' + str(val)
            else:
                self.local_other_info_dict[
                    'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' = ' + str(val)
                filterAttribs.append((tabname, attrib, 'equal', val, val))
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
        else:
            self.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
        # update table so that result is not empty
        cur = self.global_conn.cursor()
        cur.execute("Truncate table " + tabname + ';')
        # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
            """

    def handle_int_filter(self, attrib, d_plus_value, filterAttribs, tabname):
        pass
        """

        # NUMERIC HANDLING
        # min and max domain values (initialize based on data type)
        min_val_domain, max_val_domain = get_min_max_vals("int")
        flag_min = checkAttribValueEffect(tabname, attrib,
                                          min_val_domain)  # True implies row was still present
        flag_max = checkAttribValueEffect(tabname, attrib,
                                          max_val_domain)  # True implies row was still present
        # inference based on flag_min and flag_max
        if flag_max == True and flag_min == True:
            self.local_other_info_dict['Conclusion'] = 'No filter on attribute ' + attrib
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
        elif flag_min == False and flag_max == False:
            self.local_other_info_dict[
                'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator between'
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for Int filter(range) attribute..', attrib)
            equalto_flag = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]) - 1,
                                             int(d_plus_value[attrib]) + 1, '=')
            if equalto_flag:
                filterAttribs.append(
                    (tabname, attrib, '=', int(d_plus_value[attrib]), int(d_plus_value[attrib])))
                self.local_other_info_dict[
                    'Conclusion'] = u'Filter predicate is \u2013 ' + attrib + ' = ' + str(
                    d_plus_value[attrib])
                self.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                    self.local_instance_no - 1)] = copy.deepcopy(
                    self.local_other_info_dict)
            else:
                val1 = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]), max_val_domain - 1,
                                         '<=')
                val2 = getIntFilterValue(tabname, attrib, min_val_domain + 1, int(d_plus_value[attrib]),
                                         '>=')
                filterAttribs.append((tabname, attrib, 'range', int(val2), int(val1)))
                self.local_other_info_dict[
                    'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' between ' + str(
                    val2) + ' and ' + str(val1)
                self.global_other_info_dict['filter_' + attrib + '_D_mut' + str(
                    self.local_instance_no - 1)] = copy.deepcopy(
                    self.local_other_info_dict)
        elif flag_min == True and flag_max == False:
            self.local_other_info_dict[
                'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator <='
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for Int filter attribute', attrib)
            val = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]), max_val_domain - 1,
                                    '<=')
            filterAttribs.append((tabname, attrib, '<=', int(min_val_domain), int(val)))
            self.local_other_info_dict[
                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' <= ' + str(val)
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
        elif flag_min == False and flag_max == True:
            self.local_other_info_dict[
                'Conclusion'] = 'Filter predicate on ' + attrib + ' with operator >='
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            print('identifying value for Int filter attribute', attrib)
            val = getIntFilterValue(tabname, attrib, min_val_domain + 1, int(d_plus_value[attrib]),
                                    '>=')
            filterAttribs.append((tabname, attrib, '>=', int(val), int(max_val_domain)))
            self.local_other_info_dict[
                'Conclusion'] = u'Filter Predicate is \u2013 ' + attrib + ' >= ' + str(val)
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
    """


"""

# SUPPORT FUNCTIONS FOR FILTER PREDICATES

def update_other_data(tabname, attrib, attrib_type, val, result, other_info_list):
    self.local_other_info_dict = {}
    if 'text' not in attrib_type and other_info_list != []:
        low = str(other_info_list[0])
        mid = str(other_info_list[1])
        high = str(other_info_list[2])
        low_next = str(other_info_list[3])
        high_next = str(other_info_list[4])
        self.local_other_info_dict['Current Search Range'] = '[' + low + ', ' + high + ']'
        self.local_other_info_dict[
            'Current Mutation'] = 'Mutation of attribute ' + attrib + ' with value ' + str(val)
        self.local_other_info_dict['Result Cardinality'] = (len(result) - 1)
        self.local_other_info_dict['New Search Range'] = '[' + low_next + ', ' + high_next + ']'
    else:
        self.local_other_info_dict[
            'Current Mutation'] = 'Mutation of attribute ' + attrib + ' with value ' + str(val)
        self.local_other_info_dict['Result Cardinality'] = str(len(result) - 1)
    temp = copy.deepcopy(self.global_min_instance_dict[tabname])
    index = temp[0].index(attrib)
    mutated_list = copy.deepcopy(list(temp[1]))
    mutated_list[index] = str(val)
    temp[1] = mutated_list
    for tab in self.global_core_relations:
        self.global_min_instance_dict[
            'filter_' + attrib + '_' + tab + '_D_mut' + str(self.local_instance_no)] = \
            self.global_min_instance_dict[tab]
    self.global_min_instance_dict[
        'filter_' + attrib + '_' + tabname + '_D_mut' + str(self.local_instance_no)] = temp
    self.global_result_dict[
        'filter_' + attrib + '_D_mut' + str(self.local_instance_no)] = copy.deepcopy(result)
    self.local_other_info_dict['Result Cardinality'] = str(len(result) - 1)
    self.local_instance_list.append('D_mut' + str(self.local_instance_no))
    self.global_other_info_dict[
        'filter_' + attrib + '_D_mut' + str(self.local_instance_no)] = copy.deepcopy(
        self.local_other_info_dict)
    self.local_instance_no += 1


def checkAttribValueEffect(tabname, attrib, val):
    # updatequery
    query = "update " + tabname + " set " + attrib + " = " + str(val) + ";"
    cur = self.global_conn.cursor()
    cur.execute(query)
    cur.close()
    new_result = executable_aman.getExecOutput(self)
    if len(new_result) <= 1:
        cur = self.global_conn.cursor()
        cur.execute("Truncate Table " + tabname + ";")
        cur.close()
        cur = self.global_conn.cursor()
        # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
    update_other_data(tabname, attrib, 'int', val, new_result, [])
    if len(new_result) > 1:
        return True
    return False


# mukul
def getFloatFilterValue(tabname, filter_attrib, min_val, max_val, operator):
    query_front = "update " + str(tabname) + " set " + str(filter_attrib) + " = "
    cur = self.global_conn.cursor()
    cur.execute("Truncate Table " + tabname + ";")
    # conn.commit()
    cur.close()
    cur = self.global_conn.cursor()
    # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
    # conn.commit()
    cur.close()
    if operator == '<=':
        low = float(min_val)
        high = float(max_val)
        while (high - low) > 0.00001:
            mid_val = (low + high) / 2
            print("[low,high,mid]", low, high, mid_val)
            # updatequery
            query = query_front + " " + str(round(mid_val, 2)) + " ;"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            print(new_result, mid_val)
            if len(new_result) <= 1:
                # put filter_
                update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val - 0.01])
                high = mid_val - 0.01
            else:
                # put filter_
                update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, mid_val, high])
                low = mid_val
            cur = self.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return (low)

    if operator == '>=':
        low = float(min_val)
        high = float(max_val)
        while (high - low) > 0.00001:
            mid_val = (low + high) / 2
            print("[low,high,mid]", low, high, mid_val)
            # updatequery
            query = query_front + " " + str(round(mid_val, 2)) + " ;"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            if len(new_result) <= 1:
                # put filter_
                update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, mid_val + 0.01, high])
                low = mid_val + 0.01
            else:
                # put filter_
                update_other_data(tabname, filter_attrib, 'float', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val])
                high = mid_val
            cur = self.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return (high)
    return False


def getIntFilterValue(tabname, filter_attrib, min_val, max_val, operator):
    counter = 0
    query_front = "update " + tabname + " set " + filter_attrib + " = "
    query_back = ""
    firstflag = True
    cur = self.global_conn.cursor()
    cur.execute("Truncate Table " + tabname + ";")
    # conn.commit()
    cur.close()
    cur = self.global_conn.cursor()
    # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    # conn.commit()
    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
    cur.close()
    if operator == '<=':
        low = min_val
        high = max_val
        while (high - low) > 0:
            mid_val = int(math.ceil((low + high) / 2))
            # updatequery
            query = query_front + " " + str(mid_val) + " " + query_back + ";"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            if len(new_result) <= 1:
                # put filter_
                update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val - 1])
                high = mid_val - 1
            else:
                # put filter_
                update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, mid_val, high])
                low = mid_val
            cur = self.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return str(low)
    if operator == '>=':
        low = min_val
        high = max_val
        while (high - low) > 0:
            mid_val = int((low + high) / 2)
            # updatequery
            query = query_front + " " + str(mid_val) + " " + query_back + ";"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            if len(new_result) <= 1:
                # put filter_
                update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, mid_val + 1, high])
                low = mid_val + 1
            else:
                # put filter_
                update_other_data(tabname, filter_attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val])
                high = mid_val
            cur = self.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return str(high)
    if operator == '=':
        low = min_val
        high = max_val
        flag_low = True
        flag_high = True
        # updatequery
        query = query_front + " " + str(low) + " " + query_back + ";"
        cur = self.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable_aman.getExecOutput(self)
        if len(new_result) <= 1:
            flag_low = False
        # put filter_
        update_other_data(tabname, filter_attrib, 'int', low, new_result, [])
        query = query_front + " " + str(high) + " " + query_back + ";"
        cur = self.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable_aman.getExecOutput(self)
        # put filter_
        update_other_data(tabname, filter_attrib, 'int', high, new_result, [])
        cur = self.global_conn.cursor()
        cur.execute('TRUNCATE table ' + tabname + ';')
        # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        if len(new_result) <= 1:
            flag_high = False
        return (flag_low == False and flag_high == False)
    return False


def getDateFilterValue(tabname, attrib, min_val, max_val, operator):
    counter = 0
    query_front = "update " + tabname + " set " + attrib + " = "
    query_back = ""
    firstflag = True
    cur = self.global_conn.cursor()
    cur.execute("Truncate Table " + tabname + ";")
    cur.close()
    cur = self.global_conn.cursor()
    # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
    cur.close()
    if operator == '<=':
        low = min_val
        high = max_val
        while int((high - low).days) > 0:
            mid_val = low + datetime.timedelta(days=int(math.ceil(((high - low).days) / 2)))
            # updatequery
            query = query_front + " '" + str(mid_val) + "' " + query_back + ";"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            if len(new_result) <= 1:
                update_other_data(tabname, attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, low, mid_val - datetime.timedelta(days=1)])
                high = mid_val - datetime.timedelta(days=1)
            else:
                update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, mid_val, high])
                low = mid_val
            cur = self.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return low
    if operator == '>=':
        low = min_val
        high = max_val
        while int((high - low).days) > 0:
            mid_val = low + datetime.timedelta(days=int(((high - low).days) / 2))
            # updatequery
            query = query_front + " '" + str(mid_val) + "' " + query_back + ";"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            if len(new_result) <= 1:
                update_other_data(tabname, attrib, 'int', mid_val, new_result,
                                  [low, mid_val, high, mid_val + datetime.timedelta(days=1), high])
                low = mid_val + datetime.timedelta(days=1)
            else:
                update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, low, mid_val])
                high = mid_val
            cur = self.global_conn.cursor()
            cur.execute('TRUNCATE table ' + tabname + ';')
            # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
            cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
            cur.close()
        return high
    if operator == '=':
        low = min_val
        high = max_val
        flag_low = True
        flag_high = True
        # updatequery
        query = query_front + " '" + str(low) + "' " + query_back + ";"
        cur = self.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable_aman.getExecOutput(self)
        update_other_data(tabname, attrib, 'int', low, new_result, [])
        if len(new_result) <= 1:
            flag_low = False
        query = query_front + " '" + str(high) + "' " + query_back + ";"
        cur = self.global_conn.cursor()
        cur.execute(query)
        cur.close()
        new_result = executable_aman.getExecOutput(self)
        update_other_data(tabname, attrib, 'int', high, new_result, [])
        cur = self.global_conn.cursor()
        cur.execute('TRUNCATE table ' + tabname + ';')
        # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        if len(new_result) <= 1:
            flag_high = False
        return (flag_low == False and flag_high == False)
    return False


def checkStringPredicate(tabname, attrib):
    # updatequery
    val = ''
    if self.global_d_plus_value[attrib] is not None and self.global_d_plus_value[attrib][0] == 'a':
        query = "update " + tabname + " set " + attrib + " = " + "'b';"
        val = 'b'
    else:
        query = "update " + tabname + " set " + attrib + " = " + "'a';"
        val = 'a'
    cur = self.global_conn.cursor()
    cur.execute(query)
    cur.close()
    new_result = executable_aman.getExecOutput(self)
    update_other_data(tabname, attrib, 'text', val, new_result, [])
    if len(new_result) <= 1:
        cur = self.global_conn.cursor()
        cur.execute("Truncate Table " + tabname + ";")
        # conn.commit()
        cur.close()
        cur = self.global_conn.cursor()
        # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        # conn.commit()
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        return True
    query = "update " + tabname + " set " + attrib + " = " + "' ';"
    cur = self.global_conn.cursor()
    cur.execute(query)
    cur.close()
    new_result = executable_aman.getExecOutput(self)
    update_other_data(tabname, attrib, 'text', "''", new_result, [])
    if len(new_result) <= 1:
        cur = self.global_conn.cursor()
        cur.execute("Truncate Table " + tabname + ";")
        cur.close()
        cur = self.global_conn.cursor()
        # cur.execute("copy " + tabname + " from " + "'" + self.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.execute("Insert into " + tabname + " Select * from " + tabname + "4;")
        cur.close()
        return True
    return False


def getStrFilterValue(tabname, attrib, representative, max_length):
    index = 0
    output = ""
    # currently inverted exclaimaination is being used assuming it will not be in the string
    # GET minimal string with _
    while (index < len(representative)):
        temp = list(representative)
        if temp[index] == 'a':
            temp[index] = 'b'
        else:
            temp[index] = 'a'
        temp = ''.join(temp)
        # updatequery
        query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
        cur = self.global_conn.cursor()
        cur.execute(query)
        # conn.commit()
        cur.close()
        new_result = executable_aman.getExecOutput(self)
        update_other_data(tabname, attrib, 'text', temp, new_result, [])
        if len(new_result) > 1:
            self.local_other_info_dict['Conclusion'] = "'" + representative[
                index] + "' is a replacement for wildcard character '%' or '_'"
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            temp = copy.deepcopy(representative)
            temp = temp[:index] + temp[index + 1:]
            # updatequery
            query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
            cur = self.global_conn.cursor()
            cur.execute(query)
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            update_other_data(tabname, attrib, 'text', temp, new_result, [])
            if len(new_result) > 1:
                self.local_other_info_dict['Conclusion'] = "'" + representative[
                    index] + "' is a replacement from wildcard character '%'"
                self.global_other_info_dict[
                    'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                    self.local_other_info_dict)
                representative = representative[:index] + representative[index + 1:]
            else:
                self.local_other_info_dict['Conclusion'] = "'" + representative[
                    index] + "' is a replacement from wildcard character '_'"
                self.global_other_info_dict[
                    'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                    self.local_other_info_dict)
                output = output + "_"
                representative = list(representative)
                representative[index] = u"\u00A1"
                representative = ''.join(representative)
                index = index + 1
        else:
            self.local_other_info_dict['Conclusion'] = "'" + representative[
                index] + "' is an intrinsic character in filter value"
            self.global_other_info_dict[
                'filter_' + attrib + '_D_mut' + str(self.local_instance_no - 1)] = copy.deepcopy(
                self.local_other_info_dict)
            output = output + representative[index]
            index = index + 1
    if output == '':
        return output
    # GET % positions
    index = 0
    representative = copy.deepcopy(output)
    if (len(representative) < max_length):
        output = ""
        while index < len(representative):
            temp = list(representative)
            if temp[index] == 'a':
                temp.insert(index, 'b')
            else:
                temp.insert(index, 'a')
            temp = ''.join(temp)
            # updatequery
            query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
            cur = self.global_conn.cursor()
            cur.execute(query)
            # conn.commit()
            cur.close()
            new_result = executable_aman.getExecOutput(self)
            update_other_data(tabname, attrib, 'text', temp, new_result, [])
            if len(new_result) > 1:
                output = output + '%'
            output = output + representative[index]
            index = index + 1
        temp = list(representative)
        if temp[index - 1] == 'a':
            temp.append('b')
        else:
            temp.append('a')
        temp = ''.join(temp)
        # updatequery
        query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
        cur = self.global_conn.cursor()
        cur.execute(query)
        # conn.commit()
        cur.close()
        new_result = executable_aman.getExecOutput(self)
        update_other_data(tabname, attrib, 'text', temp, new_result, [])
        if len(new_result) > 1:
            output = output + '%'
    return output
"""
