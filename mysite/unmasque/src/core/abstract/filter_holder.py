from mysite.unmasque.src.core.abstract.un2_where_clause import UN2WhereClause


class FilterHolder(UN2WhereClause):
    def __init__(self, connectionHelper,
                 core_relations,
                 global_min_instance_dict, filter_extractor, name):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, name)
        self.filter_extractor = filter_extractor

        # get all methods from filter object
        self.mutate_global_min_instance_dict = self.filter_extractor.mutate_global_min_instance_dict
        self.restore_d_min_from_dict = self.filter_extractor.restore_d_min_from_dict
        self.insert_into_dmin_dict_values = self.filter_extractor.insert_into_dmin_dict_values
        self.get_datatype = self.filter_extractor.get_datatype
        self.get_dmin_val_of_attrib_list = self.filter_extractor.get_dmin_val_of_attrib_list
