from ...util.Log import Log
from ....src.core.abstract.abstractConnection import AbstractConnectionHelper


class TpchSanitizer:

    def __init__(self, connectionHelper: AbstractConnectionHelper):
        self.all_relations = []
        self.connectionHelper = connectionHelper
        self.logger = Log("TpchSanitizer", connectionHelper.config.log_level)

    def set_all_relations(self, relations: list[str]):
        self.all_relations.extend(relations)

    def take_backup(self):
        tables = self.all_relations  # self.connectionHelper.get_all_tables_for_restore()
        for table in tables:
            self.backup_one_table(table)

    def sanitize(self):
        self.connectionHelper.begin_transaction()
        tables = self.all_relations  # self.connectionHelper.get_all_tables_for_restore()
        for table in tables:
            self.sanitize_one_table(table)
        self.connectionHelper.commit_transaction()
        all_sizes = {}
        for table in tables:
            all_sizes[table] = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.get_row_count(table), self.logger)
        print(all_sizes)

    def restore_one_table(self, table):
        self.drop_derived_relations(table)
        drop_fn = self.get_drop_fn(table)
        backup_name = self.connectionHelper.queries.get_backup(table)
        self.connectionHelper.execute_sql([drop_fn(table),
                                           self.connectionHelper.queries.create_table_as_select_star_from(table,
                                                                                                          backup_name)],
                                          self.logger)

    def backup_one_table(self, table):
        self.logger.debug(f"Backing up {table}...")
        self.drop_derived_relations(table)
        backup_name = self.connectionHelper.queries.get_backup(table)
        self.connectionHelper.execute_sqls_with_DictCursor([self.connectionHelper.queries.create_table_as_select_star_from(backup_name,
                                                                                                          table)],
                                          self.logger)
        self.logger.debug(f"... done")


    def sanitize_one_table(self, table):
        self.restore_one_table(table)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table("temp"),
                                           self.connectionHelper.queries.drop_view("r_e"),
                                           self.connectionHelper.queries.drop_table("r_h")], self.logger)

    def get_drop_fn(self, table):
        return self.connectionHelper.queries.drop_table_cascade \
            if self.connectionHelper.is_view_or_table(table) == 'table' else self.connectionHelper.queries.drop_view

    def drop_derived_relations(self, table):
        derived_objects = [self.connectionHelper.queries.get_tabname_1(table),
                           self.connectionHelper.queries.get_tabname_4(table),
                           self.connectionHelper.queries.get_tabname_un(table),
                           self.connectionHelper.queries.get_tabname_nep(table),
                           self.connectionHelper.queries.get_restore_name(table),
                           table + "2",
                           table + "3"]
        drop_fns = [self.get_drop_fn(tab) for tab in derived_objects]
        for n in range(len(derived_objects)):
            drop_object = derived_objects[n]
            drop_command = drop_fns[n]
            self.connectionHelper.execute_sql([drop_command(drop_object)])
