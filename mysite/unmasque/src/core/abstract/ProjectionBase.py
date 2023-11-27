import copy

from mysite.unmasque.refactored.abstract.GenerationPipeLineBase import GenerationPipeLineBase
from mysite.unmasque.refactored.util.utils import isQ_result_empty, find_diff_idx
from mysite.unmasque.src.core.abstract.dataclass.projection_data_class import ProjectionData


class ProjectionBase(GenerationPipeLineBase, ProjectionData):

    def __init__(self, connectionHelper, name, global_all_attribs, global_attrib_types, global_key_attributes,
                 core_relations, join_graph, filter_predicates, global_min_instance_dict, attribs_to_check=None, skip_equals=True):
        ProjectionData.__init__(self)
        GenerationPipeLineBase.__init__(self, connectionHelper, name, core_relations, global_all_attribs,
                                        global_attrib_types, join_graph, filter_predicates,
                                        global_min_instance_dict, global_key_attributes)
        if attribs_to_check is None:
            self.attribs_to_check = self.global_attrib_types
        else:
            self.attribs_to_check = attribs_to_check
        self.skip_equals = skip_equals

    def find_dep_one_round(self, query, s_values):
        projected_attrib, projection_names, projection_dep, check = self.find_projection_dependencies(query, s_values)
        if not check:
            self.logger.error("Some problem while identifying the dependency list!")
        return projected_attrib, projection_names, projection_dep, check

    def find_projection_dependencies(self, query, s_values):
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            self.logger.error("Unmasque: \n some error in generating new database. "
                              "Result is empty. Can not identify "
                              "projections completely.")
            return [], [], [], False

        projection_names = list(new_result[0])
        new_result = list(new_result[1])

        projected_attrib = []
        keys_to_skip = []
        projection_dep = []
        for i in range(len(projection_names)):
            projection_dep.append([])

        s_value_dict = {}

        for entry in self.attribs_to_check:
            tabname = entry[0]
            attrib = entry[1]
            self.logger.debug("checking for ", tabname, attrib)
            if attrib not in self.global_key_attributes:
                val = self.check_impact_of_non_key_attribs(attrib, new_result, projection_dep, query,
                                                           tabname)
            else:
                val, keys_to_skip = self.check_impact_of_key_attribs(attrib, new_result, projection_dep, query, tabname,
                                                                     keys_to_skip, s_value_dict)
            if attrib not in keys_to_skip:
                s_values.append((tabname, attrib, val))

        for i in range(len(projection_names)):
            if len(projection_dep[i]) == 1:
                attrib_tup = projection_dep[i][0]
                projected_attrib.append(attrib_tup[1])
            else:
                projected_attrib.append('')

        return projected_attrib, projection_names, projection_dep, True

    def update_attrib_to_see_impact(self, attrib, tabname):
        prev = self.connectionHelper.execute_sql_fetchone_0(f"SELECT {attrib} FROM {tabname};")
        val = self.get_different_val(attrib, tabname, prev)
        self.logger.debug("update ", tabname, attrib, "with value ", val, " prev", prev)
        self.update_with_val(attrib, tabname, val)
        return val, prev

    def check_impact_of_non_key_attribs(self, attrib, new_result, projection_dep, query, tabname):
        if self.skip_equals:
            for fe in self.global_filter_predicates:
                if fe[1] == attrib and (fe[2] == 'equal' or fe[2] == '='):
                    return
        val, prev = self.update_attrib_to_see_impact(attrib, tabname)
        new_result1 = self.app.doJob(query)
        if len(new_result1) > 1:
            new_result1 = list(new_result1[1])
            diff = find_diff_idx(new_result1, new_result)
            if diff:
                for d in diff:
                    projection_dep[d].append((tabname, attrib))
            self.update_with_val(attrib, tabname, prev)
        return val

    def check_impact_of_key_attribs(self, attrib, new_result, projection_dep, query, tabname,
                                    keys_to_skip, s_value_dict):
        if attrib in keys_to_skip:
            return s_value_dict[attrib], keys_to_skip
        other_attribs = []
        join_tabnames = []
        for join_edge in self.global_join_graph:
            if attrib in join_edge:
                other_attribs = copy.deepcopy(join_edge)
                other_attribs.remove(attrib)
                break
        if other_attribs:
            val, prev = self.update_attrib_to_see_impact(attrib, tabname)
            for other_attrib in other_attribs:
                join_tabname = self.find_tabname_for_given_attrib(other_attrib)
                join_tabnames.append(join_tabname)
                self.logger.debug("update ", join_tabname, other_attrib, "with value ", val, " prev", prev)
                self.update_with_val(other_attrib, join_tabname, val)
            new_result1 = self.app.doJob(query)
            self.update_with_val(attrib, tabname, prev)
            for i in range(len(other_attribs)):
                self.update_with_val(other_attribs[i], join_tabnames[i], prev)
            if len(new_result1) > 1:
                new_result1 = list(new_result1[1])
                diff = find_diff_idx(new_result1, new_result)
                if diff:
                    for d in diff:
                        projection_dep[d].append((tabname, attrib))
            keys_to_skip = keys_to_skip + other_attribs
            for other_attrib in other_attribs:
                s_value_dict[other_attrib] = val
        else:
            val = self.check_impact_of_non_key_attribs(attrib, new_result, projection_dep, query, tabname)
        return val, keys_to_skip