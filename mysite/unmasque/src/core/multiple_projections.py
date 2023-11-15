from datetime import date

from mysite.unmasque.src.core.abstract.ProjectionBase import ProjectionBase
from mysite.unmasque.src.core.abstract.dataclass.projection_data_class import ProjectionData


def get_different_value(param):
    if isinstance(param, str):
        return param[::-1]
    if isinstance(param, date):
        return date(param.year, param.day, param.month)
    else:
        return param * -1


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
