import copy

from mysite.unmasque.refactored.projection import Projection
from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase


class ManyProjection(ExtractorModuleBase):
    def __init__(self, connectionHelper,
                 global_all_attribs,
                 global_attrib_types,
                 global_key_attributes,
                 core_relations,
                 all_join_edges,
                 subquery_data,
                 global_min_instance_dict,
                 attribs_to_check):
        super().__init__(connectionHelper, "Multiple Projection")
        self.subquery_data = subquery_data
        self.projection_extractor = Projection(connectionHelper, global_attrib_types, core_relations,
                                               attribs_to_check, all_join_edges,
                                               global_all_attribs, global_min_instance_dict, global_key_attributes)

    def doActualJob(self, query):
        if len(self.subquery_data) > 1:
            self.projection_extractor.need_full_extraction = False
        check = self.projection_extractor.doJob(query)
        if check:
            self.fill_in_data_fields(self.projection_extractor.projected_attribs,
                                     self.projection_extractor.projection_names)
        return check

    def fill_in_data_fields(self, projected_attribs, projection_names):
        for i in range(len(self.subquery_data)):
            subquery = self.subquery_data[i]
            subquery.projection.projected_attribs = copy.deepcopy(projected_attribs)
            subquery.projection.projection_names = copy.deepcopy(projection_names)
            subquery.projection.global_all_attribs = copy.deepcopy(self.projection_extractor.global_all_attribs)
            remove_idx = []
            for attrib in projected_attribs:
                x = 0
                for f in subquery.filter.filter_predicates:
                    if f[1] == attrib:
                        remove_idx.append(x)
                    x += 1
                for k in sorted(remove_idx, reverse=True):
                    del subquery.filter.filter_predicates[k]

'''
class MultipleProjection(ProjectionBase):
    def __init__(self, connectionHelper,
                 global_all_attribs,
                 global_attrib_types,
                 global_key_attributes,
                 core_relations,
                 join_graph,
                 filterData_list,
                 possibleProjectionFilter_list,
                 global_min_instance_dict,
                 attribs_to_check):
        super().__init__(connectionHelper, "Multiple Projection", global_all_attribs, global_attrib_types,
                         global_key_attributes, core_relations, join_graph, possibleProjectionFilter_list,
                         global_min_instance_dict, attribs_to_check, False)
        self.min_instance_dict_list = global_min_instance_dict
        self.possibleProjectionFilter_list = possibleProjectionFilter_list
        self.filterData = filterData_list
        self.projectionData = []
        self.subquery_count = 1

    def doExtractJob(self, query):
        s_values = []
        self.projected_attribs, self.projection_names, projection_dep, check = self.find_dep_one_round(query, s_values)
        if not check:
            return False
        for i in range(self.subquery_count):
            pData = ProjectionData()
            pData.projected_attribs = self.projected_attribs
            pData.projection_names = self.projection_names
            self.projectionData.append(pData)
            filterData = self.filterData[i]
            remove_idx = []
            for attrib in self.projected_attribs:
                x = 0
                for f in filterData.filter_predicates:
                    if f[1] == attrib:
                        remove_idx.append(x)
                    x += 1
                for k in sorted(remove_idx, reverse=True):
                    del filterData.filter_predicates[k]
        return True

    def get_different_val(self, attrib, tabname, prev):
        return get_different_value(prev)
'''