import copy

from ...util.Log import Log
from ...util.constants import UNMASQUE
from ....src.core.abstract.abstractConnection import AbstractConnectionHelper
from typing import List


class TpchSanitizer:

    def __init__(self, connectionHelper: AbstractConnectionHelper, all_sizes=None):
        if all_sizes is None:
            all_sizes = {}
        self.all_sizes = all_sizes
        self.all_relations = []
        self.connectionHelper = connectionHelper
        self.logger = Log("TpchSanitizer", connectionHelper.config.log_level)

    def set_all_relations(self, relations: List[str]):
        self.all_relations.extend(copy.copy(relations))

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

    def restore_one_table(self, table):
        self.drop_derived_relations(table)
        drop_fn = self.get_drop_fn(table)
        backup_name = self.connectionHelper.queries.get_backup(table)
        self.connectionHelper.execute_sql([drop_fn(table),
                                           self.connectionHelper.queries.create_table_like(table, backup_name),
                                           self.connectionHelper.queries.insert_into_tab_select_star_fromtab(
                                               table, backup_name)],
                                          self.logger)

    def backup_one_table(self, table):
        self.logger.debug(f"Backing up {table}...")
        self.connectionHelper.begin_transaction()
        self.drop_derived_relations(table)
        backup_name = self.connectionHelper.queries.get_backup(table)
        self.connectionHelper.execute_sqls_with_DictCursor(
            [self.connectionHelper.queries.create_table_like(backup_name, table),
             self.connectionHelper.queries.insert_into_tab_select_star_fromtab(backup_name, table)],
            self.logger)
        self.connectionHelper.commit_transaction()
        self.logger.debug(f"... done")

    def sanitize_one_table(self, table):
        self.restore_one_table(table)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table("temp"),
                                           self.connectionHelper.queries.drop_view("r_e"),
                                           self.connectionHelper.queries.drop_table("r_h")])

    def get_drop_fn(self, table):
        return self.connectionHelper.queries.drop_table_cascade \
            if self.connectionHelper.is_view_or_table(table) == 'table' else self.connectionHelper.queries.drop_view

    def drop_derived_relations(self, table):
        derived_tables = self.connectionHelper.execute_sql_fetchall(f"select tablename from pg_tables "
                                                                    f"where schemaname = '{self.connectionHelper.config.schema}' "
                                                                    f"and tablename LIKE '{table}%{UNMASQUE}';")[0]
        derived_views = self.connectionHelper.execute_sql_fetchall(f"select viewname from pg_views "
                                                                   f"where schemaname = '{self.connectionHelper.config.schema}' "
                                                                   f"and viewname LIKE '{table}%{UNMASQUE}';")[0]
        derived_objects = derived_tables + derived_views
        for obj in derived_objects:
            drop_fn = self.get_drop_fn(obj)
            self.connectionHelper.execute_sql([drop_fn(obj)])

    def get_all_sizes(self):
        for tab in self.all_relations:
            row_count = self.connectionHelper.execute_sql_fetchone_0(
                self.connectionHelper.queries.get_row_count(tab))
            self.all_sizes[tab] = row_count
        return self.all_sizes
