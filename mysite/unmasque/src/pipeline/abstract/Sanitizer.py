from mysite.unmasque.refactored.util.common_queries import get_restore_name, drop_table, alter_table_rename_to


class Sanitizer:
    def __init__(self, connectionHelper):
        self.connectionHelper = connectionHelper

    def restore_tables(self):
        res, desc = self.connectionHelper.execute_sql_fetchall(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = '" + self.connectionHelper.config.schema + "' and TABLE_CATALOG= '" + self.connectionHelper.db + "';")

        if res[0][0] > 8:
            print("Database needs to be restored!")

        res, desc = self.connectionHelper.execute_sql_fetchall(
            "SELECT SPLIT_PART(table_name, '_', 1) as original_name" +
            " FROM information_schema.tables " +
            "WHERE table_schema = '" + self.connectionHelper.config.schema + "' and TABLE_CATALOG= '" + self.connectionHelper.db
            + "' and table_name like '%_restore';")
        for row in res:
            table = row[0]
            restore_name = get_restore_name(table)
            self.connectionHelper.execute_sql([drop_table(table),
                                               alter_table_rename_to(restore_name, table)])

        res, desc = self.connectionHelper.execute_sql_fetchall("SELECT table_name"
                                                               + " FROM information_schema.tables " +
                                                               "WHERE table_schema = '" + self.connectionHelper.config.schema + "' and TABLE_CATALOG= '" + self.connectionHelper.db
                                                               + "' and table_name ~ '^[a-zA-Z]+[0-9]+$';")
        for row in res:
            self.connectionHelper.execute_sql([drop_table(row[0])])

