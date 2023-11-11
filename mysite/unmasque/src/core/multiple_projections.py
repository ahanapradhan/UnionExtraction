from datetime import datetime, date

from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase


def find_common_items(lists):
    # Use the first list as the base for comparison
    common_items = set(lists[0])
    print("common_items: ", common_items)

    # Iterate through the remaining lists
    for lst in lists[1:]:
        print("lst: ", lst)
        # Update common_items by taking the intersection with the current list
        common_items.intersection_update(set(lst))
        print("common_items: ", common_items)

    return list(common_items)


def get_different_value(param):
    if isinstance(param, str):
        return param[::-1]
    if isinstance(param, date):
        return date(param.year, param.day, param.month)
    else:
        return param * -1


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

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        new_s_values = []
        predicates = []
        for each_filter in self.filterData_list:
            predicates.append(frozenset(each_filter.filter_predicates))
        common_filters = find_common_items(predicates)
        print(common_filters)
        for s_value in common_filters:
            new_val = get_different_value(s_value[4])
            new_filter = (s_value[0], s_value[1], s_value[2], new_val, new_val)
            new_s_values.append(new_filter)



        return True
