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
                 all_filter_predicates,
                 subquery_data,
                 global_min_instance_dict,
                 attribs_to_check, is_intersection):
        super().__init__(connectionHelper, "Multiple Projection")
        self.subquery_data = subquery_data
        self.intersection = is_intersection
        if not self.intersection:
            attribs_to_check = global_all_attribs
        self.projection_extractor = Projection(connectionHelper, global_attrib_types, core_relations,
                                               all_filter_predicates, all_join_edges,
                                               global_all_attribs, global_min_instance_dict,
                                               global_key_attributes, attribs_to_check)

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        if self.intersection:
            self.projection_extractor.need_full_extraction = False
            self.projection_extractor.skip_equals = False
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
