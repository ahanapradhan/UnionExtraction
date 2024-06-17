from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util.Log import Log
from ...test.util import tpchSettings


class TPCHRestore:
    user_schema = "public"
    backup_schema = "tpch_restore"

    def __init__(self, conn: AbstractConnectionHelper):
        self.conn = conn
        self.relations = tpchSettings.relations
        self.logger = Log("Test Schema Restore", conn.config.log_level)

    def doJob(self):
        self.conn.connectUsingParams()
        self.conn.begin_transaction()
        tables = self.conn.execute_sql_fetchall(
            f"SELECT tablename from pg_tables where schemaname = '{self.user_schema}';")
        for table in tables[0]:
            self.conn.execute_sql([f"DROP TABLE {self.user_schema}.{table[0]};"])
        views = self.conn.execute_sql_fetchall(
            f"SELECT viewname from pg_views where schemaname = '{self.user_schema}';")
        for view in views[0]:
            self.conn.execute_sql([f"DROP VIEW {self.user_schema}.{view[0]};"])
        '''
        self.conn.execute_sql([f"drop schema {self.user_schema} cascade;",
                              f"create schema {self.user_schema};"], self.logger)
        '''
        for tab in self.relations:
            self.conn.execute_sql(
                [f"create table if not exists {self.user_schema}.{tab} as select * from {self.backup_schema}.{tab};",
                 f"ALTER TABLE {self.user_schema}.{tab} SET (autovacuum_enabled = false);"], self.logger)
        self.conn.commit_transaction()
        self.conn.closeConnection()
