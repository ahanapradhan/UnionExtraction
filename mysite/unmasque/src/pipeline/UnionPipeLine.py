from time import sleep

from .OuterJoinPipeLine import OuterJoinPipeLine
from ..core.union import Union
from ..util.constants import UNION, START, DONE, RUNNING, WRONG, FROM_CLAUSE


class UnionPipeLine(OuterJoinPipeLine):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper)
        self.all_relations = None
        self.name = "Union PipeLine"
        self.pipeLineError = False

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
        self.__update_time_profile(union, union_profile)
        self.core_relations = [item for subset in p for item in subset]
        self.all_relations = union.all_relations
        self.all_sizes = union.all_sizes
        self.key_lists = union.key_lists
        self.logger.debug(f"relations, {self.all_relations} all sizes, {self.all_sizes}, key list: {self.key_lists}")
        u_eq = []

        for rels in p:
            core_relations = [r for r in rels]
            self.logger.debug(core_relations)
            self.info[FROM_CLAUSE] = core_relations

            nullify = set(self.all_relations).difference(core_relations)

            self.connectionHelper.connectUsingParams()
            self.__nullify_relations(nullify)
            eq, time_profile = self._after_from_clause_extract(query, core_relations)
            self.__revert_nullifications(nullify)
            self.connectionHelper.closeConnection()

            # if time_profile is not None:
            #    self.time_profile.update(time_profile)

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
        u_Q = "\n UNION ALL \n".join(u_eq) + ";"
        if "UNION ALL" not in u_Q and u_Q.startswith('(') and u_Q.endswith(');'):
            u_Q = u_Q[1:-2] + ';'
        result = ""
        if self.pipeLineError:
            result = f"Could not extract the query due to errors {self.state}." \
                     f"\nHere's what I have as a half-baked answer:\n{pstr}\n"
            self.update_state(WRONG)
        result += u_Q
        self.update_state(DONE)
        return result

    def __update_time_profile(self, union, union_time):
        duration, app_calls = union_time[0], union_time[1]
        self.time_profile.update_for_from_clause(union.local_elapsed_time - duration, union.app_calls - app_calls)
        self.time_profile.update_for_union(duration, app_calls)

    def __nullify_relations(self, relations):
        for tab in relations:
            backup_name = self.connectionHelper.queries.get_tabname_un(tab)
            self.connectionHelper.execute_sql([self.connectionHelper.queries.alter_table_rename_to(tab, backup_name),
                                               self.connectionHelper.queries.create_table_like(tab, backup_name)],
                                              self.logger)

    def __revert_nullifications(self, relations):
        for tab in relations:
            self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table(tab),
                                               self.connectionHelper.queries.alter_table_rename_to(
                                                   self.connectionHelper.queries.get_tabname_un(tab), tab)],
                                              self.logger)
