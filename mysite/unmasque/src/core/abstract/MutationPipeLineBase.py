import copy
from _decimal import Decimal

from .AppExtractorBase import AppExtractorBase
from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper


class MutationPipeLineBase(AppExtractorBase):

    def __init__(self, connectionHelper: AbstractConnectionHelper,
                 core_relations: list[str],
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
            pass
            res, des = self.connectionHelper.execute_sql_fetchall(self.connectionHelper.queries.get_star(tab))
            self.logger.debug(f"-----  {tab} ------")
            self.logger.debug(res)
        self.logger.debug("======================")

    def restore_d_min(self):
        for tab in self.core_relations:
            self.connectionHelper.execute_sql([
                self.connectionHelper.queries.truncate_table(tab),
                self.connectionHelper.queries.insert_into_tab_select_star_fromtab(
                    tab, self.connectionHelper.queries.get_tabname_4(tab))])

    def extract_params_from_args(self, args):
        return args[0]

    def get_dmin_val(self, attrib: str, tab: str):
        res, des, val = None, None, None
        data_problem = False
        try:
            res, des = self.connectionHelper.execute_sql_fetchall(self.connectionHelper.queries.select_attribs_from_relation([attrib], tab))
            val = res[0][0]
        except ValueError:
            data_problem = True
            pass
        except IndexError:
            data_problem = True
            pass
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
