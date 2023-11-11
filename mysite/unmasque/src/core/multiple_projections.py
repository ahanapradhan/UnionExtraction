from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase


def intersect(*d):
    result = set(d[0]).intersection(*d[1:])
    return result


class MultipleProjection(ExtractorModuleBase):

    def __init__(self, connectionHelper,
                 global_key_lists,
                 fromData_list, joinData_list, filterData_list,
                 min_instance_dict_list):
        super().__init__(connectionHelper, "MultipleProjection")
        self.global_key_lists = global_key_lists
        self.fromData_list = fromData_list
        self.joinData_list = joinData_list
        self.min_instance_dict_list = min_instance_dict_list
        self.filterData_list = filterData_list
        self.projectionData = []
        self.projection_extractor = None

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        predicates = []
        for each_filter in self.filterData_list:
            predicates.append(each_filter.filter_predicates)
        common_filters = intersect(predicates)
        print(common_filters)
