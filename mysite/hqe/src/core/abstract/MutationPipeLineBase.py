import copy
from _decimal import Decimal
from abc import ABC

from .AppExtractorBase import AppExtractorBase
from ....src.core.abstract.abstractConnection import AbstractConnectionHelper
from typing import List


class MutationPipeLineBase(AppExtractorBase, ABC):

    def __init__(self, connectionHelper: AbstractConnectionHelper,
                 core_relations: List[str],
                 global_min_instance_dict: dict,
                 name: str):
        super().__init__(connectionHelper, name)
        # from from clause
        self.core_relations = core_relations
        # from view minimizer
        self.global_min_instance_dict = copy.deepcopy(global_min_instance_dict)
        self.mock = False

    def see_d_min(self):
        self.logger.debug("======================")
        for tab in self.core_relations:
            self.see_d_min_tab(tab)
        self.logger.debug("======================")

    def see_d_min_tab(self, tab):
        res, des = self.connectionHelper.execute_sql_fetchall(self.connectionHelper.queries.get_star(tab))
        self.logger.debug(f"-----  {tab} ------")
        self.logger.debug(res)

    def restore_d_min(self):
        for tab in self.core_relations:
            self.connectionHelper.execute_sql([
                self.connectionHelper.queries.truncate_table(tab),
                self.connectionHelper.queries.insert_into_tab_select_star_fromtab(
                    tab, self.connectionHelper.queries.get_dmin_tabname(tab))])

    def extract_params_from_args(self, args):
        return args[0]

    def get_dmin_val(self, attrib: str, tab: str):
        res, des, val = None, None, None
        data_problem = False
        try:
            res, des = self.connectionHelper.execute_sql_fetchall(
                self.connectionHelper.queries.select_attribs_from_relation([attrib], tab))
            val = res[0][0]
        except ValueError as e:
            data_problem = True
            self.logger.debug(e, "Could not fetch data from d_min, getting it from local dmin_instance_dict")
        except IndexError as e:
            data_problem = True
            self.logger.debug(e, "Could not fetch data from d_min, getting it from local dmin_instance_dict")
        except Exception as e:
            data_problem = True
            self.logger.debug(e, "Could not fetch data from d_min, getting it from local dmin_instance_dict")
        if data_problem:
            values = self.global_min_instance_dict[tab]
            attribs, vals = values[0], values[1]
            attrib_idx = attribs.index(attrib)
            val = vals[attrib_idx]
        ret_val = float(val) if isinstance(val, Decimal) else val
        return ret_val

    def truncate_core_relations(self):
        for table in self.core_relations:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.truncate_table(table)])
