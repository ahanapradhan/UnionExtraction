from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util.Log import Log
from ...test.util import tpchSettings


class TPCHRestore:
    backup_schema = "tpcds_restore"

    def __init__(self, conn: AbstractConnectionHelper):
        self.conn = conn
        self.relations = tpchSettings.relations
        self.logger = Log("Test Schema Restore", conn.config.log_level)

    def doJob(self):
        self.conn.connectUsingParams()

        tables = self.conn.execute_sql_fetchall(
            f"SELECT tablename from pg_tables where schemaname = '{self.conn.config.schema}' "
            f"and tableowner = '{self.conn.config.user}';")
        for table in tables[0]:
            try:
                self.conn.begin_transaction()
                self.conn.execute_sql([f"DROP TABLE {self.conn.config.schema}.{table[0]} cascade;"])
            except Exception as e:
                self.conn.rollback_transaction()
                print(e)
            else:
                self.conn.commit_transaction()
        views = self.conn.execute_sql_fetchall(
            f"SELECT viewname from pg_views where schemaname = '{self.conn.config.schema}' and "
            f"viewowner = '{self.conn.config.user}';")
        for view in views[0]:
            try:
                self.conn.begin_transaction()
                self.conn.execute_sql([f"DROP VIEW {self.conn.config.schema}.{view[0]} cascade;"])
            except Exception as e:
                self.conn.rollback_transaction()
                print(e)
            else:
                self.conn.commit_transaction()
        '''
        self.conn.execute_sql([f"drop schema {self.user_schema} cascade;",
                              f"create schema {self.user_schema};"], self.logger)
        '''
        self.conn.begin_transaction()
        for tab in self.relations:
            self.conn.execute_sql(
                [f"create table if not exists {self.conn.config.schema}.{tab} "
                 f"as select * from {self.backup_schema}.{tab};",
                 f"ALTER TABLE {self.conn.config.schema}.{tab} SET (autovacuum_enabled = false);"], self.logger)
        self.conn.commit_transaction()
        self.conn.closeConnection()
