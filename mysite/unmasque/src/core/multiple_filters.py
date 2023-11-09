from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.abstract.MutationPipeLineBase import MutationPipeLineBase
from mysite.unmasque.refactored.filter import Filter
from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase
from mysite.unmasque.src.core.abstract.dataclass.filter_data_class import FilterData


class MultipleFilter(ExtractorModuleBase):
    def __init__(self, connectionHelper,
                 global_key_lists,
                 fromData_list, joinData_list,
                 global_min_instance_dict):
        super().__init__(connectionHelper, "MultipleFilter")
        self.filter_extractors = []
        self.filterData = []

        for i in range(len(fromData_list)):
            subquery_core_relations = fromData_list[i].core_relations
            filter_extractor = Filter(self.connectionHelper, global_key_lists, subquery_core_relations,
                                      global_min_instance_dict, joinData_list[i].global_key_attributes)
            self.filter_extractors.append(filter_extractor)

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        for fl in self.filter_extractors:
            fl.doJob(query)
            self.fill_in_data_fields(fl)
        return True

    def fill_in_data_fields(self, fl):
        filter_data = FilterData()
        filter_data.filter_predicates = fl.filter_predicates
        filter_data.global_key_attributes = fl.global_key_attributes
        filter_data.global_attrib_types = fl.global_attrib_types
        self.filterData.append(filter_data)
