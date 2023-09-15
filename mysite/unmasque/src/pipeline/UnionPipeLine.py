from .ExtractionPipeLine import ExtractionPipeLine
from .abstract.generic_pipeline import GenericPipeLine
from ..core.union import Union
from ..util.constants import UNION, START, DONE, RUNNING
from ...refactored.util.common_queries import alter_table_rename_to, create_table_like, drop_table, \
    get_restore_name, get_tabname_4, get_tabname_un


class UnionPipeLine(GenericPipeLine):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Union PipeLine")
        self.old_pipeline = ExtractionPipeLine(self.connectionHelper)

    def extract(self, query):
        # opening and closing connection actions are vital.
        self.connectionHelper.connectUsingParams()

        self.update_state(UNION + START)
        union = Union(self.connectionHelper)
        self.update_state(UNION + RUNNING)
        p, pstr = union.doJob(query)
        self.update_state(UNION + DONE)
        self.time_profile.update_for_union(union.local_elapsed_time)
        all_relations = union.all_relations
        key_lists = union.key_lists

        self.connectionHelper.closeConnection()

        u_eq = []
        pipeLineError = False

        for rels in p:
            core_relations = []
            for r in rels:
                core_relations.append(r)
            print(core_relations)

            nullify = set(all_relations).difference(core_relations)

            self.connectionHelper.connectUsingParams()
            self.nullify_relations(nullify)
            eq, time_profile = self.old_pipeline.after_from_clause_extract(query, all_relations,
                                                                           core_relations, key_lists)
            self.revert_nullifications(nullify)
            self.connectionHelper.closeConnection()

            if eq is not None:
                print(eq)
                eq = eq.replace('Select', '(Select')
                eq = eq.replace(';', ')')
                u_eq.append(eq)
            else:
                pipeLineError = True
                break

            if time_profile is not None:
                self.time_profile.update(time_profile)

        u_Q = "\n UNION ALL \n".join(u_eq)
        u_Q += ";"

        result = ""
        if pipeLineError:
            result = "Could not extract the query due to errors.\nHere's what I have as a half-baked answer:\n" + pstr + "\n"
        result += u_Q

        self.update_state(DONE)
        return result

    def nullify_relations(self, relations):
        for tab in relations:
            self.connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_un(tab)),
                                               create_table_like(tab, get_tabname_un(tab))])

    def revert_nullifications(self, relations):
        for tab in relations:
            self.connectionHelper.execute_sql([drop_table(tab),
                                               alter_table_rename_to(get_tabname_un(tab), tab),
                                               drop_table(get_tabname_un(tab))])

    def revert_sideEffects(self, relations):
        for tab in relations:
            self.connectionHelper.execute_sql([drop_table(tab),
                                               alter_table_rename_to(get_restore_name(tab), tab),
                                               drop_table(get_tabname_4(tab))])

    def get_state(self):
        if super().get_state() == UNION + DONE:
            return self.old_pipeline.get_state()
