from ....src.core.abstract.abstractConnection import AbstractConnectionHelper


class TpchSanitizer:

    def __init__(self, connectionHelper: AbstractConnectionHelper):
        self.all_relations = []
        self.connectionHelper = connectionHelper

    def set_all_relations(self, relations: list[str]):
        self.all_relations.extend(relations)

    def sanitize(self):
        self.connectionHelper.begin_transaction()
        tables = self.connectionHelper.get_all_tables_for_restore()
        for table in tables:
            self.drop_derived_relations(table)
            drop_fn = self.get_drop_fn(table)
            restore_name = self.connectionHelper.queries.get_restore_name(table)
            self.connectionHelper.execute_sql([drop_fn(table),
                                               self.connectionHelper.queries.alter_table_rename_to(restore_name, table)])
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table("temp"),
                                           self.connectionHelper.queries.drop_view("r_e"),
                                           self.connectionHelper.queries.drop_table("r_h")])
        self.connectionHelper.commit_transaction()

    def get_drop_fn(self, table):
        return self.connectionHelper.queries.drop_table_cascade \
            if self.connectionHelper.is_view_or_table(table) == 'table' else self.connectionHelper.queries.drop_view

    def drop_derived_relations(self, table):
        derived_objects = [self.connectionHelper.queries.get_tabname_1(table),
                           self.connectionHelper.queries.get_tabname_4(table),
                           self.connectionHelper.queries.get_tabname_un(table),
                           self.connectionHelper.queries.get_tabname_nep(table),
                           table + "2",
                           table + "3"]
        drop_fns = [self.get_drop_fn(tab) for tab in derived_objects]
        for n in range(len(derived_objects)):
            drop_object = derived_objects[n]
            drop_command = drop_fns[n]
            self.connectionHelper.execute_sql([drop_command(drop_object)])
