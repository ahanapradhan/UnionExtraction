import ast
import copy
import random
from datetime import date, timedelta
from typing import Union, Tuple

from .MutationPipeLineBase import MutationPipeLineBase
from ...util.utils import get_unused_dummy_val, get_dummy_val_for, get_format, get_char, get_escape_string
from ....src.core.abstract.abstractConnection import AbstractConnectionHelper
from ....src.core.dataclass.generation_pipeline_package import PackageForGenPipeline

NUMBER_TYPES = ['int', 'integer', 'numeric', 'float', 'number', 'Decimal']
NON_TEXT_TYPES = ['date'] + NUMBER_TYPES


def generate_random_date(lb, ub):
    start_date = lb
    end_date = ub
    # Generate two random dates
    random_date1 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date1


class GenerationPipeLineBase(MutationPipeLineBase):

    def __init__(self, connectionHelper: AbstractConnectionHelper, name: str, genCtx: PackageForGenPipeline):
        super().__init__(connectionHelper, genCtx.core_relations, genCtx.global_min_instance_dict, name)
        self.global_all_attribs = genCtx.global_all_attribs
        self.global_attrib_types = genCtx.global_attrib_types
        self.global_join_graph = genCtx.global_join_graph
        self.global_filter_predicates = genCtx.global_filter_predicates
        self.filter_attrib_dict = genCtx.filter_attrib_dict
        self.attrib_types_dict = genCtx.attrib_types_dict
        self.joined_attribs = genCtx.joined_attribs

        self.get_datatype = genCtx.get_datatype  # method

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args=None) -> bool:
        query = self.extract_params_from_args(args)
        self.do_init()
        check = self.doExtractJob(query)
        return check

    def restore_d_min_from_dict(self) -> None:
        for tab in self.core_relations:
            values = self.global_min_instance_dict[tab]
            attribs, vals = values[0], values[1]
            for i in range(len(attribs)):
                attrib, val = attribs[i], vals[i]
                self.update_with_val(attrib, tab, val)

    def do_init(self) -> None:
        for tab in self.core_relations:
            self.connectionHelper.execute_sql(
                [self.connectionHelper.queries.create_table_as_select_star_from_limit_1(f"{tab}__temp",
                                                                                        self.connectionHelper.queries.get_backup(
                                                                                            tab)),
                 self.connectionHelper.queries.drop_table(tab),
                 self.connectionHelper.queries.alter_table_rename_to(f"{tab}__temp", tab)])
        self.restore_d_min_from_dict()
        self.see_d_min()

    def get_s_val_for_textType(self, attrib_inner, tabname_inner) -> str:
        filtered_val = self.filter_attrib_dict[(tabname_inner, attrib_inner)]
        if isinstance(filtered_val, tuple):
            filtered_val = filtered_val[0]
        return filtered_val

    def insert_attrib_vals_into_table(self, att_order, attrib_list_inner,
                                      insert_rows, tabname_inner, insert_logger=True) -> None:
        esc_string = get_escape_string(attrib_list_inner)
        insert_query = self.connectionHelper.queries.insert_into_tab_attribs_format(att_order, esc_string,
                                                                                    tabname_inner)
        if insert_logger:
            self.connectionHelper.execute_sql_with_params(insert_query, insert_rows, self.logger)
        else:
            self.connectionHelper.execute_sql_with_params(insert_query, insert_rows)

    def update_attrib_in_table(self, attrib, value, tabname) -> None:
        update_query = self.connectionHelper.queries.update_tab_attrib_with_value(tabname, attrib, value)
        self.connectionHelper.execute_sql([update_query])

    def doExtractJob(self, query: str) -> bool:
        return True

    def get_other_attribs_in_eqJoin_grp(self, attrib: str) -> list:
        other_attribs = []
        for join_edge in self.global_join_graph:
            if attrib in join_edge:
                other_attribs = copy.deepcopy(join_edge)
                other_attribs.remove(attrib)
                break
        return other_attribs

    def update_attribs_bulk(self, join_tabnames, other_attribs, val) -> None:
        for other_attrib in other_attribs:
            join_tabname = self.find_tabname_for_given_attrib(other_attrib)
            join_tabnames.append(join_tabname)
            self.update_with_val(other_attrib, join_tabname, val)

    def update_attrib_to_see_impact(self, attrib: str, tabname: str) \
            -> Tuple[Union[int, float, date, str], Union[int, float, date, str]]:
        prev = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.select_attribs_from_relation([attrib], tabname))
        val = self.get_different_s_val(attrib, tabname, prev)
        self.logger.debug(f"update {tabname}.{attrib} with value {val} that had previous value {prev}")
        self.update_with_val(attrib, tabname, val)
        return val, prev

    def update_with_val(self, attrib: str, tabname: str, val) -> None:
        if val == 'NULL':
            update_q = self.connectionHelper.queries.update_tab_attrib_with_value(tabname, attrib, val)
        else:
            datatype = self.get_datatype((tabname, attrib))
            if datatype in NON_TEXT_TYPES:
                update_q = self.connectionHelper.queries.update_tab_attrib_with_value(tabname, attrib,
                                                                                      get_format(datatype, val))
            else:
                update_q = self.connectionHelper.queries.update_tab_attrib_with_quoted_value(tabname, attrib, val)
        self.connectionHelper.execute_sql([update_q])

    def get_s_val(self, attrib: str, tabname: str) -> Union[int, float, date, str]:
        datatype = self.get_datatype((tabname, attrib))
        if datatype in NON_TEXT_TYPES:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = min(self.filter_attrib_dict[(tabname, attrib)][0],
                          self.filter_attrib_dict[(tabname, attrib)][1])
            else:
                val = get_dummy_val_for(datatype)
            # val = ast.literal_eval(get_format(datatype, val))
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_s_val_for_textType(attrib, tabname)
                self.logger.debug(val)
                val = val.replace('%', '')
            else:
                val = get_char(get_dummy_val_for('char'))
        return val

    def get_other_than_dmin_val_nonText(self, attrib: str, tabname: str, prev) -> Union[int, float, date, str]:
        datatype = self.get_datatype((tabname, attrib))
        key = (tabname, attrib)
        if key not in self.filter_attrib_dict:
            return get_dummy_val_for(datatype)

        if len(self.filter_attrib_dict[key]) == 1:
            self.logger.info("Cannot generate a new s-val. Giving the old one!")
            return prev

        lb, ub = self.filter_attrib_dict[key][0], self.filter_attrib_dict[key][1]
        if prev == lb:
            val = ub
        elif prev == ub:
            val = lb
        else:
            val = min(lb, ub)
        return val

    def get_different_s_val(self, attrib: str, tabname: str, prev) -> Union[int, float, date, str]:
        datatype = self.get_datatype((tabname, attrib))
        self.logger.debug(f"datatype of {attrib} is {datatype}")
        if datatype in NON_TEXT_TYPES:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_other_than_dmin_val_nonText(attrib, tabname, prev)
            else:
                val = get_unused_dummy_val(datatype, [prev])
            if datatype == 'date':
                val = ast.literal_eval(get_format(datatype, val))
        else:
            if (tabname, attrib) in self.filter_attrib_dict.keys():
                val = self.get_s_val_for_textType(attrib, tabname)
                self.logger.debug(val)
                val = val.replace('%', '')
            else:
                val = get_char(get_unused_dummy_val('char', [prev]))
        return val

    def find_tabname_for_given_attrib(self, find_attrib) -> str:
        for entry in self.global_attrib_types:
            tabname = entry[0]
            attrib = entry[1]
            if attrib == find_attrib:
                return tabname
