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
        self.conn.execute_sql([f"drop schema {self.user_schema} cascade;",
                               f"create schema {self.user_schema};"], self.logger)
        for tab in self.relations:
            # print(f"Recreating {tab}")
            self.conn.execute_sql(
                [f"create table {self.user_schema}.{tab} as select * from {self.backup_schema}.{tab};",
                 f"ALTER TABLE {self.user_schema}.{tab} SET (autovacuum_enabled = false);"
                 "commit;"], self.logger)
        self.conn.closeConnection()
