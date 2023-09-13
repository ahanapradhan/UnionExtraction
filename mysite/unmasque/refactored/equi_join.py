import copy

from .util.utils import get_datatype_from_typesList, get_dummy_val_for, get_val_plus_delta, \
    get_all_combo_lists
from .abstract.where_clause import WhereClause


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


def remove_edge_from_join_graph_dicts(curr_list, list1, list2, global_key_lists):
    for keys in global_key_lists:
        if all(x in keys for x in curr_list):
            global_key_lists.remove(keys)
    global_key_lists.append(copy.deepcopy(list1))
    global_key_lists.append(copy.deepcopy(list2))


class EquiJoin(WhereClause):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 core_relations,
                 global_min_instance_dict):
        super().__init__(connectionHelper,
                         global_key_lists,
                         core_relations,
                         global_min_instance_dict)
        # join data
        self.global_join_instance_dict = {}
        self.global_component_dict = {}
        self.global_join_graph = []
        self.global_key_attributes = []

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.get_init_data()
        self.get_join_graph(query)
        return self.global_join_graph

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

    def get_join_graph(self, query):
        global_key_lists = copy.deepcopy(self.global_key_lists)
        join_graph = []
        attrib_types_dict, combo_dict_of_lists = self.construct_attribs_types_dict()

        # For each list, test its presence in join graph
        # This will either add the list in join graph or break it
        while global_key_lists:
            curr_list = global_key_lists[0]
            join_keys = [join_key for join_key in curr_list if join_key[0] in self.core_relations]
            if len(join_keys) <= 1:
                global_key_lists.remove(curr_list)
                continue
            print("... checking for: ", join_keys)

            # Try for all possible combinations
            for elt in combo_dict_of_lists[len(join_keys)]:
                list1, list2, list_type = construct_two_lists(attrib_types_dict, join_keys, elt)
                val1, val2 = get_two_different_vals(list_type)
                temp_copy = {tab: self.global_min_instance_dict[tab] for tab in self.core_relations}

                # Assign two different values to two lists in database
                self.assign_values_to_lists(list1, list2, temp_copy, val1, val2)

                # CHECK THE RESULT
                new_result = self.app.doJob(query)
                if len(new_result) > 1:
                    remove_edge_from_join_graph_dicts(join_keys, list1, list2, global_key_lists)
                    break

            for keys in global_key_lists:
                if all(x in keys for x in join_keys):
                    global_key_lists.remove(keys)
                    join_graph.append(copy.deepcopy(join_keys))

            for val in join_keys:
                self.connectionHelper.execute_sql(["Insert into " + val[0] + " Select * from " + val[0] + "4;"])
        self.refine_join_graph(join_graph)

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
