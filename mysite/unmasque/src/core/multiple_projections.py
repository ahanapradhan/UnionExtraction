import copy
from datetime import date

from mysite.unmasque.refactored.util.common_queries import update_tab_attrib_with_value_where, \
    update_tab_attrib_with_quoted_value_where
from mysite.unmasque.refactored.util.utils import isQ_result_empty, find_diff_idx, get_format
from mysite.unmasque.src.core.abstract.ProjectionBase import ProjectionBase


def complementary_char(char):
    if 'A' <= char <= 'Z':
        return chr(ord('A') + ord('Z') - ord(char))
    elif 'a' <= char <= 'z':
        return chr(ord('a') + ord('z') - ord(char))
    else:
        return char.swapcase()


def get_different_value(param):
    if param is None:
        return None
    if isinstance(param, str):
        if len(param) == 1:
            change = complementary_char(param)
            return change
        return param[::-1]
    if isinstance(param, date):
        return date(param.year - 1, param.month, param.day)
    else:
        return param * -1


def merge_output(init_list, output_list):
    if init_list is None:
        return copy.deepcopy(output_list)
    for i in range(len(init_list)):
        if init_list[i] == '' and output_list[i] != '':
            init_list[i] = output_list[i]
    return init_list


def remove_specific_filter(logger, subquery):
    remove_idx = []
    logger.debug("filter:", subquery.filter.filter_predicates)
    for attrib in subquery.projection.projected_attribs:
        if attrib == '':
            continue
        x = 0
        for f in subquery.filter.filter_predicates:
            if f[1] == attrib:
                remove_idx.append(x)
            x += 1
    for k in sorted(remove_idx, reverse=True):
        logger.debug(f"remove filter {str(subquery.filter.filter_predicates[k])}")
        del subquery.filter.filter_predicates[k]
    logger.debug("filter:", subquery.filter.filter_predicates)


class ManyProjection(ProjectionBase):
    def __init__(self, connectionHelper,
                 global_all_attribs,
                 global_attrib_types,
                 global_key_attributes,
                 core_relations,
                 all_join_edges,
                 subquery_data,
                 global_min_instance_dict,
                 attribs_to_check):
        super().__init__(connectionHelper, "Multiple Projection", global_all_attribs, global_attrib_types,
                         global_key_attributes, core_relations, all_join_edges, None, global_min_instance_dict,
                         attribs_to_check)
        self.subquery_data = subquery_data
        self.attrib_to_check_dict = attribs_to_check

    def get_where_key_val_sql(self, tab, i):
        subquery = self.subquery_data[i]
        key = subquery.filter.tab_key_value_dict[tab][0]
        value = subquery.filter.tab_key_value_dict[tab][1]
        where_condition = f"where {key} = {value}"
        return where_condition

    def update_with_val(self, tab_attrib, val):
        tabname, attrib, i = tab_attrib[0], tab_attrib[1], tab_attrib[2]
        wherekey = self.get_where_key_val_sql(tabname, i)
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = update_tab_attrib_with_value_where(attrib, tabname, get_format('date', val), wherekey)
        elif 'int' in self.attrib_types_dict[(tabname, attrib)] \
                or 'numeric' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = update_tab_attrib_with_value_where(attrib, tabname, val, wherekey)
        else:
            update_q = update_tab_attrib_with_quoted_value_where(tabname, attrib, val, wherekey)
        self.connectionHelper.execute_sql([update_q])

    def update_attrib_to_see_impact(self, tab_attrib):
        tabname, attrib, i = tab_attrib[0], tab_attrib[1], tab_attrib[2]
        wherekey = self.get_where_key_val_sql(tabname, i)
        prev = self.connectionHelper.execute_sql_fetchone_0(f"SELECT {attrib} FROM {tabname} {wherekey};")
        val = self.get_different_val(attrib, tabname, prev)
        self.logger.debug("update ", tabname, attrib, "with value ", val, " prev", prev)
        self.update_with_val(tab_attrib, val)
        return val, prev

    def find_projection_dependencies(self, query, s_values):
        new_result = self.app.doJob(query)
        if isQ_result_empty(new_result):
            self.logger.error("Unmasque: \n some error in generating new database. "
                              "Result is empty. Can not identify "
                              "projections completely.")
            return [], [], [], False

        projection_names = [list(new_result[0]) for _ in range(len(self.subquery_data))]
        new_result = list(new_result[1])

        projected_attrib = [[] for _ in range(len(self.subquery_data))]
        keys_to_skip = [[] for _ in range(len(self.subquery_data))]
        projection_dep = []
        for _ in range(len(self.subquery_data)):
            subquery_projection_dep = []
            for _ in range(len(projection_names[0])):
                subquery_projection_dep.append([])
            projection_dep.append(subquery_projection_dep)

        s_value_dict = [{} for _ in range(len(self.subquery_data))]
        val_cache_dict = {}

        for i in range(len(self.subquery_data)):  # entry is a list of list of tuples
            entry = self.attribs_to_check[i][0]  # as of now assume only one attribute involved in projection
            tab_attrib = (entry[0], entry[1], i)
            val_cache_dict[i] = [entry[0], entry[1]]
            # self.logger.debug("checking for ", tabname, attrib)
            if tab_attrib[1] not in self.global_key_attributes:
                val, prev = self.update_attrib_to_see_impact(tab_attrib)
                val_cache_dict[i].append(prev)  # storing prev value with tabname and attrib

        new_result1 = self.app.doJob(query)
        if len(new_result1) > 1:
            new_result1 = list(new_result1[1])
            diff = find_diff_idx(new_result1, new_result)
            if diff:
                for i in range(len(self.subquery_data)):
                    subquery_projection_dep = projection_dep[i]
                    tabname = val_cache_dict[i][0]
                    attrib = val_cache_dict[i][1]
                    prev = val_cache_dict[i][2]

                    for d in diff:
                        subquery_projection_dep[d].append((tabname, attrib))
                    self.update_with_val((tabname, attrib, i), prev)

            '''
            Ignore Key attributes as-of-now. 
            
            else:
                val, keys_to_skip = self.check_impact_of_key_attribs(new_result, projection_dep, query, keys_to_skip,
                                                                     s_value_dict, tab_attrib)
            if tab_attrib[1] not in keys_to_skip:
                s_values.append((tab_attrib[0], tab_attrib[1], val))
            '''

        for idx in range(len(self.subquery_data)):
            subquery_projection_names = projection_names[idx]
            subquery_projection_dep = projection_dep[idx]
            subquery_projected_attrib = projected_attrib[idx]
            for i in range(len(subquery_projection_names)):
                if len(subquery_projection_dep[i]) == 1:
                    attrib_tup = subquery_projection_dep[i][0]
                    subquery_projected_attrib.append(attrib_tup[1])
                else:
                    subquery_projected_attrib.append('')

        return projected_attrib, projection_names, projection_dep, True

    def doBasicExtractJob(self, query):
        self.projected_attribs, self.projection_names, _, check = self.find_projection_dependencies(query, s_values=[])
        return check

    def doExtractJob(self, query):
        check = True
        for key in self.attrib_to_check_dict.keys():
            self.attribs_to_check = self.attrib_to_check_dict[key]
            check = check and self.doBasicExtractJob(query)
            if check:
                self.fill_in_data_fields(self.projected_attribs,
                                         self.projection_names)
        for i in range(len(self.subquery_data)):
            subquery = self.subquery_data[i]
            remove_specific_filter(self.logger, subquery)

        return check

    def get_different_val(self, attrib, tabname, prev):
        return get_different_value(prev)

    def fill_in_data_fields(self, projected_attribs, projection_names):
        for i in range(len(self.subquery_data)):
            subquery = self.subquery_data[i]
            self.fill_in_projectionData(i, projected_attribs, projection_names, subquery)
            # remove_specific_filter(self.logger, subquery)

    def fill_in_projectionData(self, i, projected_attribs, projection_names, subquery):
        subquery.projection.projected_attribs = merge_output(subquery.projection.projected_attribs,
                                                             projected_attribs[i])
        subquery.projection.projection_names = merge_output(subquery.projection.projection_names,
                                                            projection_names[i])
        if subquery.projection.global_all_attribs != self.global_all_attribs:
            subquery.projection.global_all_attribs = copy.deepcopy(self.global_all_attribs)
