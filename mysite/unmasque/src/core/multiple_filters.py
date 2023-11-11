import copy

from mysite.unmasque.refactored.filter import Filter
from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase
from mysite.unmasque.src.core.abstract.dataclass.filter_data_class import FilterData


class MultipleFilter(ExtractorModuleBase):
    def __init__(self, connectionHelper,
                 global_key_lists,
                 fromData_list, joinData_list,
                 min_instance_dict_list):
        super().__init__(connectionHelper, "MultipleFilter")
        self.global_key_lists = global_key_lists
        self.fromData_list = fromData_list
        self.joinData_list = joinData_list
        self.min_instance_dict_list = min_instance_dict_list
        self.filterData = []

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        for i in range(len(self.fromData_list)):
            # by design, this is a singleton object
            filter_extractor = Filter(self.connectionHelper, self.global_key_lists, self.fromData_list[i].core_relations,
                                      self.min_instance_dict_list[i], self.joinData_list[i].global_key_attributes)
            # so, need to explicitly assign params which are different
            self.set_params(filter_extractor, i)
            filter_extractor.doJob(query)
            self.fill_in_data_fields(filter_extractor)
        return True

    def set_params(self, filter_extractor, i):
        filter_extractor.core_relations = self.fromData_list[i].core_relations
        filter_extractor.global_min_instance_dict = self.min_instance_dict_list[i]
        filter_extractor.global_key_attributes = self.joinData_list[i].global_key_attributes

    def fill_in_data_fields(self, fl):
        filter_data = FilterData()
        filter_data.filter_predicates = copy.deepcopy(fl.filter_predicates)
        filter_data.global_key_attributes = copy.deepcopy(fl.global_key_attributes)
        filter_data.global_attrib_types = copy.deepcopy(fl.global_attrib_types)
        self.filterData.append(filter_data)
