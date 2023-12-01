import copy

from mysite.unmasque.refactored.filter import Filter
from mysite.unmasque.src.core.abstract.ExtractorModuleBase import ExtractorModuleBase


def set_extractor_params(filter_extractor, subquery):
    filter_extractor.core_relations = subquery.from_clause.core_relations
    filter_extractor.global_min_instance_dict = subquery.d_min_dict
    filter_extractor.global_key_attributes = subquery.equi_join.global_key_attributes
    filter_extractor.global_all_attribs = subquery.equi_join.global_all_attribs


class ManyFilter(ExtractorModuleBase):
    def __init__(self, connectionHelper,
                 global_key_lists,
                 subqueryData_list):
        super().__init__(connectionHelper, "MultipleFilter")
        self.global_key_lists = global_key_lists
        self.subquery_data = subqueryData_list
        subquery = self.subquery_data[0]
        self.filter_extractor = Filter(self.connectionHelper, self.global_key_lists,
                                       subquery.from_clause.core_relations,
                                       subquery.d_min_dict, subquery.equi_join.global_key_attributes)

    def repeatJob(self, query):
        for i in range(1, len(self.subquery_data)):
            subquery = self.subquery_data[i]
            set_extractor_params(self.filter_extractor, subquery)
            self.doExtractJob(query, i)

    def doExtractJob(self, query, i):
        self.filter_extractor.doJob(query)
        self.fill_in_data_fields(self.filter_extractor, i)

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.doExtractJob(query, 0)
        self.repeatJob(query)
        return True

    def fill_in_data_fields(self, fl, i):
        self.subquery_data[i].filter.filter_predicates = copy.deepcopy(fl.filter_predicates)
        self.subquery_data[i].filter.global_key_attributes = copy.deepcopy(fl.global_key_attributes)
        self.subquery_data[i].filter.global_attrib_types = copy.deepcopy(fl.global_attrib_types)
