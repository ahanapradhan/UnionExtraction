from .abstract.AppExtractorBase import AppExtractorBase
from .abstract.abstractConnection import AbstractConnectionHelper


class DbRestorer(AppExtractorBase):
    def __init__(self, connectionHelper: AbstractConnectionHelper, core_relations: list, name="Database Restorer"):
        super().__init__(connectionHelper, name)
        self.relations = []
        self.last_restored_size = {}
        self.core_relations = core_relations

    def set_all_sizes(self, sizes):
        self.all_sizes = sizes
        self.relations = list(self.all_sizes.keys())
        self.relations.sort()

    def extract_params_from_args(self, args):
        return args[0]

    def restore_one_table_where(self, table, where):
        self.drop_derived_relations(table)
        drop_fn = self.get_drop_fn(table)
        backup_name = self.connectionHelper.queries.get_backup(table)
        self.connectionHelper.execute_sql([drop_fn(table),
                                           self.connectionHelper.queries.create_table_as_select_star_from_where(table,
                                                                                                                backup_name,
                                                                                                                where)],
                                          self.logger)

    def sanitize_one_table_where(self, table, where):
        self.restore_one_table_where(table, where)
        self.connectionHelper.execute_sql([self.connectionHelper.queries.drop_table("temp"),
                                           self.connectionHelper.queries.drop_view("r_e"),
                                           self.connectionHelper.queries.drop_table("r_h")])

    def doActualJob(self, args=None):
        tabs_wheres = self.extract_params_from_args(args)
        if tabs_wheres is None:
            to_restore = self.core_relations if len(self.core_relations) else self.relations
            for tab in to_restore:
                if tab not in self.last_restored_size.keys():
                    self.update_current_sizes(tab)
                if self.last_restored_size[tab] != self.all_sizes[tab]:
                    row_count = self.restore_table_and_confirm(tab)
                    if not row_count:
                        return False
        else:
            self.logger.debug("All sizes:", self.all_sizes)
            self.logger.debug("Current sizes:", self.last_restored_size)

            self.logger.debug(tabs_wheres)
            for i, entry in enumerate(tabs_wheres):
                tab, where = entry[0], entry[1]
                if tab not in self.last_restored_size.keys():
                    self.update_current_sizes(tab)
                if where == 'true' and self.last_restored_size[tab] == self.all_sizes[tab]:
                    self.logger.info("No need to restore table")
                else:
                    check = self.restore_table_and_confirm(tab, where)
                    if not check:
                        return False
        return True

    def restore_table_and_confirm(self, tab, where=None):
        try:
            if where is None:
                self.sanitize_one_table(tab)
            else:
                self.sanitize_one_table_where(tab, where)
            row_count = self.update_current_sizes(tab)
            return row_count
        except Exception as e:
            self.logger.error("Error ", str(e))
            return 0

    def update_current_sizes(self, tab):
        row_count = self.connectionHelper.execute_sql_fetchone_0(
            self.connectionHelper.queries.get_row_count(tab))
        self.last_restored_size[tab] = row_count
        self.logger.debug("Updating table size: ", self.last_restored_size[tab])
        return row_count
