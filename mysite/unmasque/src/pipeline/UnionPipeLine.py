import datetime
import time

from .OuterJoinPipeLine import OuterJoinPipeLine
from ..core.union import Union
from ..util.constants import UNION, START, DONE, RUNNING, WRONG, FROM_CLAUSE, ERROR


class UnionPipeLine(OuterJoinPipeLine):

    def __init__(self, connectionHelper, name="Union_PipeLine"):
        super().__init__(connectionHelper, name)

    def extract(self, query):
        # opening and closing connection actions are vital.
        self.connectionHelper.connectUsingParams()
        self.update_state(UNION + START)
        union = Union(self.connectionHelper)
        self.update_state(UNION + RUNNING)
        p, pstr, union_profile = union.doJob(query)
        self.update_state(UNION + DONE)
        self.connectionHelper.closeConnection()

        self.info[UNION] = [list(ele) for ele in p]
        print(f"Now end {str(datetime.datetime.now().time())}")
        self.__update_time_profile(union, union_profile)
        self.core_relations = [item for subset in p for item in subset]
        self.all_relations = union.all_relations
        self.all_sizes = union.all_sizes
        self.key_lists = union.key_lists
        self.logger.debug(f"relations, {self.all_relations} all sizes, {self.all_sizes}, key list: {self.key_lists}")
        u_eq = []
        # return 0

        for rels in p:
            self.info[UNION] = [list(ele) for ele in p]
            core_relations = [r for r in rels]
            self.logger.debug(core_relations)
            self.info[FROM_CLAUSE] = core_relations

            nullify = set(self.all_relations).difference(core_relations)

            self.connectionHelper.connectUsingParams()
            self.__nullify_relations(nullify)
            eq = self._after_from_clause_extract(query, core_relations)
            self.__revert_nullifications(nullify)
            self.q_generator.reset()
            self.connectionHelper.closeConnection()

            if eq is not None:
                self.logger.debug(eq)
                eq = eq.replace('Select', '(Select')
                eq = eq.replace(';', ')')
                u_eq.append(eq)
            else:
                self.pipeLineError = True
                break

        result = self.__post_process(pstr, u_eq)
        return result

    def __post_process(self, pstr, u_eq):
        u_Q = "\n UNION ALL ".join(u_eq) + ";"
        if "UNION ALL" not in u_Q and u_Q.startswith('(') and u_Q.endswith(');'):
            u_Q = u_Q[1:-2] + ';'
        result = ""
        if self.pipeLineError:
            self.error += f"Could not extract the query due to errors." \
                          f"\nHere's what I have as a half-baked answer:\n{pstr}\n"
            self.update_state(ERROR)
            return None
        result += u_Q
        return result

    def __update_time_profile(self, union, union_time):
        duration, app_calls = union_time[0], union_time[1]
        self.time_profile.update_for_from_clause(union.local_elapsed_time - duration, union.app_calls - app_calls)
        self.time_profile.update_for_union(duration, app_calls)

    def __nullify_relations(self, relations):
        for tab in relations:
            f_tab, f_union_tab, union_tab, original_tab = self.__get_void_names(tab)
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(f_tab, union_tab),
                                               self.connectionHelper.queries.create_table_like(f_tab, original_tab)],
                                              self.logger)
            self.connectionHelper.commit_transaction()
            self.all_sizes[tab] = 0

    def __get_void_names(self, tab):
        union_tab = self.connectionHelper.queries.get_union_tabname(tab)
        f_union_tab = f"{self.connectionHelper.config.schema}.{union_tab}"
        f_tab = f"{self.connectionHelper.config.schema}.{tab}"
        original_tab = f"{self.connectionHelper.config.user_schema}.{tab}"
        return f_tab, f_union_tab, union_tab, original_tab

    def __revert_nullifications(self, relations):
        for tab in relations:
            f_tab, f_union_tab, union_tab, original_tab = self.__get_void_names(tab)
            self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table_cascade(f_tab),
                                               self.connectionHelper.queries.alter_table_rename_to(
                                                   f_union_tab, tab)],
                                              self.logger)
            self.connectionHelper.commit_transaction()
            self.all_sizes[tab] = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.get_row_count(f_tab), self.logger)
