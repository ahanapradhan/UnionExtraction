from typing import Literal
from ....refactored.util.common_queries import drop_view, get_restore_name, drop_table, alter_table_rename_to


class TpchSanitizer:
    TABLES = ['lineitem', 'partsupp', 'orders', 'customer', 'supplier', 'nation', 'region', 'part']

    def __init__(self, connectionHelper):
        self.connectionHelper = connectionHelper

    def get_all_relations(self):
        res, desc = self.connectionHelper.execute_sql_fetchall("SELECT table_name"
                                                               + self.from_where_catalog() + ";")
        tables = []
        for row in res:
            tables.append(row[0])
        return tables

    def from_where_catalog(self):
        return " FROM information_schema.tables " + \
            "WHERE table_schema = '" + self.connectionHelper.config.schema + "' and TABLE_CATALOG= '" \
            + self.connectionHelper.db + "'"

    def is_view_or_table(self, table_or_view_name: str) -> Literal['view', 'table']:
        # Reference: https://www.postgresql.org/docs/current/infoschema-tables.html
        check_query = "select table_type " + self.from_where_catalog() + f" and table_name = '{table_or_view_name}'"
        res, _ = self.connectionHelper.execute_sql_fetchall(check_query)

        if res[0][0] == 'VIEW':
            return 'view'
        else:
            return 'table'

    def begin_transaction(self):
        self.connectionHelper.execute_sql(["BEGIN;"])

    def commit_transaction(self):
        self.connectionHelper.execute_sql(["COMMIT;"])

    def doJob(self):
        res, desc = self.connectionHelper.execute_sql_fetchall(
            "SELECT count(*)" + self.from_where_catalog() + ";")

        if res[0][0] > len(self.TABLES):
            print("Database needs to be restored!")

        self.begin_transaction()
        res, desc = self.connectionHelper.execute_sql_fetchall("SELECT SPLIT_PART(table_name, '_', 1) as original_name"
                                                               + self.from_where_catalog()
                                                               + " and table_name like '%_restore';")
        for row in res:
            table = row[0]
            drop_fn = drop_table if self.is_view_or_table(table) == 'table' else drop_view
            restore_name = get_restore_name(table)
            self.connectionHelper.execute_sql([drop_fn(table), alter_table_rename_to(restore_name, table)])

        res, desc = self.connectionHelper.execute_sql_fetchall("SELECT table_name"
                                                               + self.from_where_catalog()
                                                               + " and table_name ~ '^[a-zA-Z]+[0-9]+$';")
        for row in res:
            self.connectionHelper.execute_sql([drop_table(row[0])])

        self.connectionHelper.execute_sql([drop_table("temp")])

        self.commit_transaction()
