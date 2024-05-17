import copy

from ....src.core.abstract.un2_where_clause import UN2WhereClause


class FilterHolder(UN2WhereClause):
    def __init__(self, connectionHelper,
                 core_relations,
                 global_min_instance_dict, filter_extractor, name):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, name)
        self.filter_extractor = filter_extractor

        # method from filter object
        # self._prepare_attrib_list = self.filter_extractor.prepare_attrib_set_for_bulk_mutation
        self._extract_filter_on_attrib_set = self.filter_extractor.extract_filter_on_attrib_set
        self.global_d_plus_values = copy.deepcopy(self.filter_extractor.global_d_plus_value)
        self.global_attrib_max_legth = copy.deepcopy(self.filter_extractor.global_attrib_max_length)

        # get all methods from filter object
        self.mutate_global_min_instance_dict = self.filter_extractor.mutate_global_min_instance_dict
        self.restore_d_min_from_dict = self.filter_extractor.restore_d_min_from_dict
        self.insert_into_dmin_dict_values = self.filter_extractor.insert_into_dmin_dict_values
        self.get_datatype = self.filter_extractor.get_datatype
        self.get_dmin_val_of_attrib_list = self.filter_extractor.get_dmin_val_of_attrib_list

    def get_dmin_val(self, attrib: str, tab: str):
        return self.global_d_plus_values[attrib]  # short-cut works since tpch has all relations distinct attrib name
