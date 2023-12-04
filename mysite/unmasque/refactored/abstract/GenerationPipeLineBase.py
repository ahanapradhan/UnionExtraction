import ast

from .MutationPipeLineBase import MutationPipeLineBase
from ..util.common_queries import insert_into_tab_attribs_format, update_tab_attrib_with_value, \
    update_tab_attrib_with_quoted_value
from ...refactored.util.utils import get_escape_string, get_dummy_val_for, get_format, get_char, get_unused_dummy_val


class GenerationPipeLineBase(MutationPipeLineBase):

    def __init__(self, connectionHelper, name, core_relations, global_all_attribs, global_attrib_types, join_graph,
                 filter_predicates, global_min_instance_dict, global_key_attributes):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, name)
        self.global_all_attribs = global_all_attribs
        self.global_attrib_types = global_attrib_types
        self.global_join_graph = join_graph
        self.global_filter_predicates = filter_predicates
        self.filter_attrib_dict = {}
        self.attrib_types_dict = {}
        self.global_key_attributes = global_key_attributes

    def extract_params_from_args(self, args):
        return args[0]

    def construct_filter_attribs_dict(self):
        filter_attrib_dict = {}
        if self.global_filter_predicates is not None:
            # get filter values and their allowed minimum and maximum value
            for entry in self.global_filter_predicates:
                if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
                    filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
                else:
                    filter_attrib_dict[(entry[0], entry[1])] = entry[3]
        return filter_attrib_dict

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.do_init()
        check = self.doExtractJob(query)
        return check

    def do_init(self):
        self.attrib_types_dict = {(entry[0], entry[1]): entry[2] for entry in self.global_attrib_types}
        self.filter_attrib_dict = self.construct_filter_attribs_dict()

    def insert_attrib_vals_into_table(self, att_order, attrib_list_inner, insert_rows, tabname_inner):
        att_order, esc_string = get_escape_string(att_order, attrib_list_inner)
        insert_query = insert_into_tab_attribs_format(att_order, esc_string, tabname_inner)
        self.connectionHelper.execute_sql_with_params(insert_query, insert_rows)

    def update_attrib_in_table(self, attrib, value, tabname):
        update_query = update_tab_attrib_with_value(attrib, tabname, value)
        self.connectionHelper.execute_sql([update_query])

    def doExtractJob(self, query):
        return True

    def update_with_val(self, attrib, tabname, val):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = update_tab_attrib_with_value(attrib, tabname, get_format('date', val))
        elif 'int' in self.attrib_types_dict[(tabname, attrib)] \
                or 'numeric' in self.attrib_types_dict[(tabname, attrib)]:
            update_q = update_tab_attrib_with_value(attrib, tabname, val)
        else:
            update_q = update_tab_attrib_with_quoted_value(tabname, attrib, val)
        self.connectionHelper.execute_sql([update_q])

    def get_val(self, attrib, tabname):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for('date')
            val = ast.literal_eval(get_format('date', val))

        elif ('int' in self.attrib_types_dict[(tabname, attrib)]
              or 'numeric' in self.attrib_types_dict[(tabname, attrib)]):
            # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for('int')
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.filter_attrib_dict[(tabname, attrib)]
                self.logger.debug(val)
                val = val.replace('%', '')
            else:
                val = get_char(get_dummy_val_for('char'))
        return val

    def get_different_val_for_dmin(self, attrib, tabname, prev):
        if prev == self.filter_attrib_dict[(tabname, attrib)][0]:
            val = self.filter_attrib_dict[(tabname, attrib)][1]
        elif prev == self.filter_attrib_dict[(tabname, attrib)][1]:
            val = self.filter_attrib_dict[(tabname, attrib)][0]
        else:
            val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                      self.filter_attrib_dict[(tabname, attrib)][1])
        return val

    def get_different_val(self, attrib, tabname, prev):
        if 'date' in self.attrib_types_dict[(tabname, attrib)]:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_different_val_for_dmin(attrib, tabname, prev)
            else:
                val = get_unused_dummy_val('date', [prev])
            val = ast.literal_eval(get_format('date', val))

        elif ('int' in self.attrib_types_dict[(tabname, attrib)]
              or 'numeric' in self.attrib_types_dict[(tabname, attrib)]):
            # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_different_val_for_dmin(attrib, tabname, prev)
            else:
                val = get_unused_dummy_val('int', [prev])
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.filter_attrib_dict[(tabname, attrib)]
                self.logger.debug(val)
                val = val.replace('%', '')
            else:
                val = get_char(get_unused_dummy_val('char', [prev]))
        return val

    def find_tabname_for_given_attrib(self, find_attrib):
        for entry in self.global_attrib_types:
            tabname = entry[0]
            attrib = entry[1]
            if attrib == find_attrib:
                return tabname
