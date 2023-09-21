from mysite.unmasque.refactored.util.common_queries import get_restore_name, drop_table, alter_table_rename_to


class TpchSanitizer:
    TABLES = ['lineitem', 'partsupp', 'orders', 'customer', 'supplier', 'nation', 'region', 'part']

    def __init__(self, connectionHelper):
        self.connectionHelper = connectionHelper

    def from_where_catalog(self):
        return " FROM information_schema.tables " + \
            "WHERE table_schema = '" + self.connectionHelper.config.schema + "' and TABLE_CATALOG= '" \
            + self.connectionHelper.db + "'"

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
            restore_name = get_restore_name(table)
            self.connectionHelper.execute_sql([drop_table(table), alter_table_rename_to(restore_name, table)])

        res, desc = self.connectionHelper.execute_sql_fetchall("SELECT table_name"
                                                               + self.from_where_catalog()
                                                               + " and table_name ~ '^[a-zA-Z]+[0-9]+$';")
        for row in res:
            self.connectionHelper.execute_sql([drop_table(row[0])])

        self.commit_transaction()
