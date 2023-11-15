from itertools import chain

from mysite.unmasque.refactored.projection import Projection
from mysite.unmasque.src.core.multiple_projections import MultipleProjection


def find_common_items(lists):
    common_items = set(lists[0])
    print("common_items: ", common_items)

    for lst in lists[1:]:
        print("lst: ", lst)
        common_items.intersection_update(set(lst))
        print("common_items: ", common_items)

    return list(common_items)


def get_merged_list(nested_list):
    flattened_list = list(set(chain(*nested_list)))
    return flattened_list


class ProjectionFactory:
    def __init__(self, connectionHelper, fromData_list, joinData_list, global_join_graph, filterData_list,
                 core_relations,
                 d_min_instance_dict):
        self.connectionHelper = connectionHelper
        self.fromData_list = fromData_list
        self.joinData_list = joinData_list
        self.global_join_graph = global_join_graph
        self.filterData_list = filterData_list
        self.core_relations = core_relations
        self.d_min_instance_dict = d_min_instance_dict

    def find_attribs_to_check(self):
        predicates = []
        for each_filter in self.filterData_list:
            predicates.append(frozenset(each_filter.filter_predicates))
        common_filters = find_common_items(predicates)
        print(common_filters)
        return common_filters

    def doCreateJob(self):
        attribs_to_check = self.find_attribs_to_check()

        if len(self.filterData_list) > 1:
            local_attrib_types = []
            global_all_attribs, global_attrib_types, global_key_attributes = self.form_other_params()

            for attrib in attribs_to_check:
                for atypes in global_attrib_types:
                    if atypes[0] == attrib[0] and atypes[1] == attrib[1]:
                        local_attrib_types.append(atypes)

            pj = MultipleProjection(self.connectionHelper,
                                    global_all_attribs,
                                    global_attrib_types,
                                    global_key_attributes,
                                    self.core_relations,
                                    self.global_join_graph,
                                    self.filterData_list,
                                    attribs_to_check,
                                    self.d_min_instance_dict,
                                    local_attrib_types)
            pj.subquery_count = len(self.fromData_list)
        else:
            pj = Projection(self.connectionHelper,
                            self.global_join_graph[0].global_attrib_types,
                            self.fromData_list[0].core_relations,
                            self.filterData_list[0].filter_predicates,
                            self.global_join_graph,
                            self.global_join_graph[0].global_all_attribs,
                            self.d_min_instance_dict,
                            self.global_join_graph[0].global_key_attributes)
        return pj

    def form_other_params(self):
        _global_all_attribs = set()
        for joindata in self.joinData_list:
            for attrib in joindata.global_all_attribs:
                _global_all_attribs.add(frozenset(attrib))
        global_all_attribs = list(_global_all_attribs)

        _global_attrib_types = set()
        for joindata in self.joinData_list:
            for attrib in joindata.global_attrib_types:
                _global_attrib_types.add(attrib)
        global_attrib_types = list(_global_attrib_types)

        _global_key_attributes = set()
        for joindata in self.joinData_list:
            for attrib in joindata.global_key_attributes:
                _global_key_attributes.add(attrib)
        global_key_attributes = list(_global_key_attributes)

        return global_all_attribs, global_attrib_types, global_key_attributes
